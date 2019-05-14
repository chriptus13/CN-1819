import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceOptions;
import com.google.cloud.WriteChannel;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.storage.*;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CNPhotosClient {
    private static final Scanner sc = new Scanner(System.in);
    private static final Pattern ptAdd = Pattern.compile("/add ([^\\s]+)"),
            ptSearchLabels = Pattern.compile("/searchLabels (.+)"),
            ptSearchFaces = Pattern.compile("/searchFaces (\\d+)");
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static final Executor poolExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static StorageOptions storageOptions = StorageOptions.getDefaultInstance();
    private static Storage storage = storageOptions.getService();
    private static FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance();
    private static Firestore db = firestoreOptions.getService();

    // /add C:\Users\Asus\Desktop/cr7.jpg

    public static void main(String[] args) {
        String topicName = "Topic_T1",
                bucketName = "cnphotos-g06";

        showHelp();
        do {
            String line = sc.nextLine();
            if (line.startsWith("/exit")) break;
            Matcher mAdd = ptAdd.matcher(line),
                    mSearchLabels = ptSearchLabels.matcher(line),
                    mSearchFaces = ptSearchFaces.matcher(line);
            if (mAdd.matches())
                addImage(topicName, bucketName, mAdd.group(1));
            else if (mSearchLabels.matches()) {
                String label = mSearchLabels.group(1);
                List<String> labels = label.indexOf(';') == -1 ? Collections.singletonList(label) : Arrays.asList(label.split(";"));
                labels.forEach(CNPhotosClient::searchImage);
            } else if (mSearchFaces.matches())
                searchImage(Integer.parseInt(mSearchFaces.group(1)));
            else if (line.startsWith("/help"))
                showHelp();
            else
                System.out.println("Invalid Command!");
        } while (true);
    }

    private static void searchImage(int numberOfFaces) {
        ApiFuture<QuerySnapshot> querySnapshot =
                db.collection("images").whereEqualTo("numberOfFaces", numberOfFaces).get();
        querySnapshot.addListener(() -> {
            try {
                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                documents.stream()
                        .map(QueryDocumentSnapshot::getData)
                        .forEach(System.out::println);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }, poolExecutor);
    }

    private static void searchImage(String label) {
        ApiFuture<QuerySnapshot> querySnapshot = db.collection("images")
                .whereArrayContains("labels", label).get();
        querySnapshot.addListener(() -> { // TODO make concurrent set to remove duplicates
            try {
                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                for (QueryDocumentSnapshot doc : documents)
                    System.out.println(doc.getData());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }, poolExecutor);
    }

    private static void addImage(String topicName, String bucketName, String img) {
        try {
            BlobId blobId = addBlobToBucket(bucketName, img);
            publishMessageOnTopic(topicName, blobId.getBucket(), blobId.getName());
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static BlobId addBlobToBucket(String bucketName, String img) throws Exception {
        Path uploadFrom = Paths.get(img);
        String contentType = Files.probeContentType(uploadFrom);
        BlobId blobId = BlobId.of(bucketName, uploadFrom.getFileName().toString());
        Acl acl = Acl.newBuilder(Acl.User.ofAllUsers(), Acl.Role.READER).build();
        List<Acl> acls = new LinkedList<>();
        acls.add(acl);
        BlobInfo blobInfo =
                BlobInfo.newBuilder(blobId).setContentType(contentType).setAcl(acls).build();
        if (Files.size(uploadFrom) < 1_000_000) {
            byte[] bytes = Files.readAllBytes(uploadFrom);
            storage.create(blobInfo, bytes);
        } else {
            try (WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[1024];
                try (InputStream input = Files.newInputStream(uploadFrom)) {
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
        }
        return blobId;
    }

    private static void publishMessageOnTopic(String topicName, String bucketName, String blobName) {
        ProjectTopicName topic = ProjectTopicName.of(PROJECT_ID, topicName);
        try {
            Publisher publisher = Publisher.newBuilder(topic).build();
            PubsubMessage msg = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(bucketName + ";;" + blobName))
                    .build();
            ApiFuture<String> fut = publisher.publish(msg);

            fut.addListener(() -> {
                try {
                    System.out.println("MESSAGE PUBLISHED with ID=" + fut.get());
                } catch (InterruptedException | ExecutionException e) {
                    System.out.println("Error: " + e.getMessage());
                } finally {
                    try {
                        publisher.shutdown();
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                }
            }, poolExecutor);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void showHelp() {
        System.out.println("*** Help ***");
        System.out.println("/help - show commands");
        System.out.println("/add <img-path> - add image to the system");
        System.out.println("/search <labels> - search images containing labels");
        System.out.println("/exit - leave");
        System.out.println("*** **** ***");
    }
}
