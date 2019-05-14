import com.google.api.core.ApiService;
import com.google.cloud.ServiceOptions;
import com.google.cloud.WriteChannel;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.storage.*;
import com.google.cloud.vision.v1.Image;
import com.google.cloud.vision.v1.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class Worker {
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static StorageOptions storageOptions = StorageOptions.getDefaultInstance();
    private static Storage storage = storageOptions.getService();
    private static FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance();
    private static Firestore db = firestoreOptions.getService();

    public static void main(String[] args) {
        String subscriptionName = "Subscription_A";
        System.out.println("Worker Application");
        subscribe(subscriptionName).awaitTerminated();
    }

    private static ApiService subscribe(String subscriptionName) {
        ProjectSubscriptionName pSubName = ProjectSubscriptionName.of(PROJECT_ID, subscriptionName);
        Subscriber subscriber = Subscriber.newBuilder(pSubName, Worker::work).build();
        ApiService service = subscriber.startAsync();
        service.awaitRunning();
        System.out.println("Subscribed!");
        return service;
    }

    private static void work(PubsubMessage msg, AckReplyConsumer ackReply) {
        // msg -> <bucketName;;blobName>
        String[] msgSplit = msg.getData().toStringUtf8().split(";;");
        ackReply.ack();

        ByteArrayOutputStream newImg = null;
        try {
            Blob blob = getBlobFromBucket(msgSplit[0], msgSplit[1]);

            AnnotateImageResponse response = process(blob);

            CNImage image;
            if (!response.getFaceAnnotationsList().isEmpty()) {
                newImg = writeWithFaces(blob, response.getFaceAnnotationsList());
                String name = blob.getName();
                BlobId blobWithFaces =
                        putBlobInBucket(blob, name.substring(0, name.indexOf('.')).concat("_WithFaces.jpg"), newImg);
                image = new CNImage(
                        blob.getBucket(),
                        blob.getName(),
                        blobWithFaces.getName(),
                        response.getFaceAnnotationsCount(),
                        response.getLabelAnnotationsList().stream()
                                .map(EntityAnnotation::getDescription)
                                .collect(Collectors.toList())
                );
            } else {
                image = new CNImage(
                        blob.getBucket(),
                        blob.getName(),
                        response.getLabelAnnotationsList().stream()
                                .map(EntityAnnotation::getDescription)
                                .collect(Collectors.toList())
                );
            }

            storeInDB(image);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (newImg != null) {
                try {
                    newImg.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void storeInDB(CNImage image) {
        CollectionReference colRef = db.collection("images");

        DocumentReference docRef = colRef.document(image.getBucketName() + image.getBlobName());
        docRef.set(image);
    }

    private static BlobId putBlobInBucket(Blob blob, String blobName, ByteArrayOutputStream newImg) throws IOException {
        BlobId blobId = BlobId.of(blob.getBucket(), blobName);
        Acl acl = Acl.newBuilder(Acl.User.ofAllUsers(), Acl.Role.READER).build();
        List<Acl> acls = new LinkedList<>();
        acls.add(acl);

        BlobInfo blobInfo =
                BlobInfo.newBuilder(blobId).setContentType("image/jpg").setAcl(acls).build();
        try (WriteChannel writer = storage.writer(blobInfo)) {
            byte[] buffer = new byte[1024];
            try (InputStream input = new ByteArrayInputStream(newImg.toByteArray())) {
                int limit;
                while ((limit = input.read(buffer)) >= 0) {
                    try {
                        writer.write(ByteBuffer.wrap(buffer, 0, limit));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        return blobId;
    }

    private static AnnotateImageResponse process(Blob blob) throws IOException {
        try (ImageAnnotatorClient vision = ImageAnnotatorClient.create()) {
            List<AnnotateImageRequest> requests = new ArrayList<>();

            ByteString imgBytes = ByteString.copyFrom(blob.getContent());
            Image img = Image.newBuilder().setContent(imgBytes).build();

            Feature faceFeat = Feature.newBuilder().setType(Feature.Type.FACE_DETECTION).build();
            Feature labelFeat = Feature.newBuilder().setType(Feature.Type.LABEL_DETECTION).build();
            AnnotateImageRequest request = AnnotateImageRequest.newBuilder()
                    .addFeatures(faceFeat)
                    .addFeatures(labelFeat)
                    .setImage(img)
                    .build();
            requests.add(request);

            // Performs label detection on the image file
            BatchAnnotateImagesResponse batchResponse = vision.batchAnnotateImages(requests);

            assert batchResponse.getResponsesList().size() == 1;

            AnnotateImageResponse response = batchResponse.getResponsesList().get(0);

            if (response.getFaceAnnotationsList() == null || response.getLabelAnnotationsList() == null) {
                throw new IOException(
                        response.getError() != null
                                ? response.getError().getMessage()
                                : "Unknown error getting image annotations");
            }

            return response;
        }
    }

    private static Blob getBlobFromBucket(String bucketName, String blobName) {
        BlobId blobId = BlobId.of(bucketName, blobName);
        return storage.get(blobId);
    }

    private static ByteArrayOutputStream writeWithFaces(Blob blob, List<FaceAnnotation> faces) throws IOException {
        InputStream in = new ByteArrayInputStream(blob.getContent());
        BufferedImage img = ImageIO.read(in);
        faces.forEach(face -> annotateWithFace(img, face));
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        ImageIO.write(img, "jpg", os);
        return os;
    }

    /**
     * Annotates an image {@code img} with a polygon defined by {@code face}.
     */
    private static void annotateWithFace(BufferedImage img, FaceAnnotation face) {
        Graphics2D gfx = img.createGraphics();
        Polygon poly = new Polygon();
        for (Vertex vertex : face.getFdBoundingPoly().getVerticesList()) {
            poly.addPoint(vertex.getX(), vertex.getY());
        }
        gfx.setStroke(new BasicStroke(5));
        gfx.setColor(new Color(0xFD5D5D5D, true));
        gfx.draw(poly);
    }
}
