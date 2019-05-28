import cnphotos.CNPhotosGrpc;
import cnphotos.InstanceLimit;
import cnphotos.MonitorState;
import cnphotos.TargetCpuUsage;
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
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

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
            ptSearchFaces = Pattern.compile("/searchFaces (\\d+)"),
            ptSetMonitorTarget = Pattern.compile("/setMonTargetPerc (\\d+)"),
            ptSetMonitorMaxMin = Pattern.compile("/setMonInstanceMinMax (\\d+) (\\d+)");
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static final Executor poolExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static StorageOptions storageOptions = StorageOptions.getDefaultInstance();
    private static Storage storage = storageOptions.getService();
    private static FirestoreOptions firestoreOptions = FirestoreOptions.getDefaultInstance();
    private static Firestore db = firestoreOptions.getService();

    private static final String svc_ip = "localhost";
    private static final int svc_port = 7000;
    private static ManagedChannel ch = ManagedChannelBuilder
            .forAddress(svc_ip, svc_port)
            .usePlaintext()
            .build();
    private static CNPhotosGrpc.CNPhotosStub stub = CNPhotosGrpc.newStub(ch);

    public static void main(String[] args) {
        String topicName = "Topic_T1",
                bucketName = "cnphotos-g06";

        showHelp();
        do {
            String line = sc.nextLine();
            if(line.startsWith("/exit")) break;
            Matcher mAdd = ptAdd.matcher(line),
                    mSearchLabels = ptSearchLabels.matcher(line),
                    mSearchFaces = ptSearchFaces.matcher(line),
                    mSetMonitorTarget = ptSetMonitorTarget.matcher(line),
                    mSetMonitorMaxMin = ptSetMonitorMaxMin.matcher(line);
            if(mAdd.matches())
                addImage(topicName, bucketName, mAdd.group(1));
            else if(mSearchLabels.matches()) {
                String label = mSearchLabels.group(1);
                List<String> labels = label.indexOf(';') == -1 ? Collections.singletonList(label) : Arrays.asList(label.split(";"));
                labels.forEach(CNPhotosClient::searchImage);
            } else if(mSearchFaces.matches())
                searchImage(Integer.parseInt(mSearchFaces.group(1)));
            else if(line.startsWith("/help"))
                showHelp();
            else if(mSetMonitorTarget.matches())
                setMonitorTargetPerc(Integer.parseInt(mSetMonitorTarget.group(1)));
            else if(mSetMonitorMaxMin.matches())
                setMonitorInstanceMaxMin(Integer.parseInt(mSetMonitorMaxMin.group(1)),
                        Integer.parseInt(mSetMonitorMaxMin.group(2)));
            else if(line.startsWith("/mon"))
                showMonitorStatus();
            else
                System.out.println("Invalid Command!");
        } while(true);
    }

    private static void showMonitorStatus() {
        stub.getMonitorState(
                Empty.newBuilder().build(),
                new StreamObserver<MonitorState>() {
                    @Override
                    public void onNext(MonitorState monitorState) {
                        System.out.println(
                                String.format("CPU usage: %.4f | %d instances",
                                        monitorState.getCpuUsage(), monitorState.getNInstances()
                                )
                        );
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("ERROR: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() { }
                }
        );

    }

    private static void setMonitorInstanceMaxMin(int min, int max) {
        stub.changeInstanceLimits(
                InstanceLimit.newBuilder().setMin(min).setMax(max).build(),
                new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty empty) { }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("ERROR: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() { }
                }
        );
    }

    private static void setMonitorTargetPerc(int targetPerc) {
        float perc = targetPerc / 100f;
        stub.changeTargetCpuUsage(
                TargetCpuUsage.newBuilder().setUsage(perc).build(),
                new StreamObserver<Empty>() {
                    @Override
                    public void onNext(Empty empty) { }

                    @Override
                    public void onError(Throwable throwable) {
                        System.out.println("ERROR: " + throwable.getMessage());
                    }

                    @Override
                    public void onCompleted() { }
                }
        );
    }

    // 6 & 7
    private static void searchImage(int numberOfFaces) {
        ApiFuture<QuerySnapshot> querySnapshot =
                db.collection("images").whereEqualTo("numberOfFaces", numberOfFaces).get();
        querySnapshot.addListener(() -> {
            try {
                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                documents.stream()
                        .map(QueryDocumentSnapshot::getData)
                        .forEach(System.out::println);
            } catch(InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }, poolExecutor);
    }


    // 6 & 7
    private static void searchImage(String label) {
        ApiFuture<QuerySnapshot> querySnapshot = db.collection("images")
                .whereArrayContains("labels", label).get();
        querySnapshot.addListener(() -> { // TODO make concurrent set to remove duplicates
            try {
                List<QueryDocumentSnapshot> documents = querySnapshot.get().getDocuments();
                for(QueryDocumentSnapshot doc : documents)
                    System.out.println(doc.getData());
            } catch(InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }, poolExecutor);
    }

    // 1
    private static void addImage(String topicName, String bucketName, String img) {
        try {
            // 2.1
            BlobId blobId = addBlobToBucket(bucketName, img);

            // 2.2
            publishMessageOnTopic(topicName, blobId.getBucket(), blobId.getName());
        } catch(Exception ex) {
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
        if(Files.size(uploadFrom) < 1_000_000) {
            byte[] bytes = Files.readAllBytes(uploadFrom);
            storage.create(blobInfo, bytes);
        } else {
            try(WriteChannel writer = storage.writer(blobInfo)) {
                byte[] buffer = new byte[1024];
                try(InputStream input = Files.newInputStream(uploadFrom)) {
                    int limit;
                    while((limit = input.read(buffer)) >= 0) {
                        try {
                            writer.write(ByteBuffer.wrap(buffer, 0, limit));
                        } catch(Exception ex) {
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
                } catch(InterruptedException | ExecutionException e) {
                    System.out.println("Error: " + e.getMessage());
                } finally {
                    try {
                        publisher.shutdown();
                    } catch(Exception e) {
                        System.out.println("Error: " + e.getMessage());
                    }
                }
            }, poolExecutor);

        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    private static void showHelp() {
        System.out.println("*** Help ***");
        System.out.println("/help - show commands");
        System.out.println("/add <img-path> - add image to the system");
        System.out.println("/search <labels> - search images containing labels");
        System.out.println("/mon - check monitor status");
        System.out.println("/setMonTargetPerc <perc> - set target cpu percentage (integer)");
        System.out.println("/setMonInstanceMinMax <min> <max> - set min and max number of VM instances");
        System.out.println("/exit - leave");
        System.out.println("*** **** ***");
    }
}
