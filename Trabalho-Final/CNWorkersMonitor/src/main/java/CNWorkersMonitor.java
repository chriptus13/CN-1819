import cnphotos.CNPhotosGrpc;
import cnphotos.InstanceLimit;
import cnphotos.MonitorState;
import cnphotos.TargetCpuUsage;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.core.ApiService;
import com.google.api.services.compute.Compute;
import com.google.api.services.compute.ComputeScopes;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;

public class CNWorkersMonitor extends CNPhotosGrpc.CNPhotosImplBase {
    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static final String ZONE_NAME = "us-east1-b";
    private static final String INSTANCE_GROUP = "cn-photos-group";
    private static final String METRICS_SUB = "CN_Photos_Metrics_Subscription";
    private static final long INTERVAL = 15_000;

    private static final Object mon = new Object();

    private static final Map<String, CpuPerc> map = new HashMap<>();

    private static final long MAX_ABOVE = 5;
    private static final long MAX_BELOW = 10;
    private static final int svcPort = 7000;
    private static long consecutiveAbove = 0;
    private static long consecutiveBelow = 0;
    private static float target = 0.5f;
    private static float avg;
    private static int currSize;
    private static Compute computeService;
    private static int minInstances = 1;
    private static int maxInstances = 10;

    public static void main(String[] args) {
        try {
            Server svc = ServerBuilder
                    .forPort(svcPort)
                    .addService(new CNWorkersMonitor())
                    .build()
                    .start();
            System.out.println("Server started, listening on " + svcPort);
            HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
            GoogleCredential credential = GoogleCredential.getApplicationDefault();
            if(credential.createScopedRequired()) {
                List<String> scopes = new ArrayList<>();
                scopes.add(ComputeScopes.COMPUTE);
                credential = credential.createScoped(scopes);
            }
            computeService = new Compute
                    .Builder(httpTransport, credential.getJsonFactory(), credential)
                    .setApplicationName("Client")
                    .build();

            ApiService subscribe = subscribe();

            System.out.print("Press any key to terminate...");
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
            subscribe.stopAsync().awaitTerminated();
            svc.shutdown();
        } catch(GeneralSecurityException | IOException e) {
            e.printStackTrace();
        }
    }

    private static ApiService subscribe() {
        ProjectSubscriptionName pSubName = ProjectSubscriptionName.of(PROJECT_ID, METRICS_SUB);
        Subscriber subscriber = Subscriber.newBuilder(pSubName, CNWorkersMonitor::work).build();
        ApiService service = subscriber.startAsync();
        service.awaitRunning();
        System.out.println("Subscribed!");
        return service;
    }

    private static void work(PubsubMessage msg, AckReplyConsumer ackReply) {
        // msg -> [hostname]:[cpu%]
        System.out.println(msg.getData().toStringUtf8());

        String[] msgSplit = msg.getData().toStringUtf8().split(":");
        String hostname = msgSplit[0];
        float perc = Float.parseFloat(msgSplit[1]);
        ackReply.ack();
        insertPerc(hostname, perc);
        try {
            updateAvg();
        } catch(IOException e) {
            System.out.println(e.getMessage());
        }
    }

    private static void insertPerc(String hostname, float perc) {
        synchronized(mon) {
            if(map.containsKey(hostname)) map.get(hostname).setPerc(perc);
            else map.put(hostname, new CpuPerc(perc));
        }
    }

    private static void updateAvg() throws IOException {
        synchronized(mon) {
            long currTime = System.currentTimeMillis();
            long minTime = currTime - INTERVAL;
            map.entrySet().stream()
                    .filter(entry -> entry.getValue().getTime() < minTime)
                    .map(Map.Entry::getKey)
                    .forEach(map::remove);

            float sum = map.values().stream()
                    .map(CpuPerc::getPerc)
                    .reduce(0f, Float::sum);

            avg = sum / map.size();

            // check if exceed

            if(avg >= target) {
                consecutiveBelow = 0;
                if(++consecutiveAbove >= MAX_ABOVE) {
                    consecutiveAbove = 0;
                    //increase size
                    increaseInstanceGroup();
                }
            } else {
                consecutiveAbove = 0;
                if(++consecutiveBelow >= MAX_BELOW) {
                    consecutiveBelow = 0;
                    //decrease size
                    decreaseInstanceGroup();
                }
            }
        }
    }

    private static void increaseInstanceGroup() throws IOException {
        currSize = computeService.instanceGroupManagers()
                .get(PROJECT_ID, ZONE_NAME, INSTANCE_GROUP).execute().getTargetSize();
        if(currSize < maxInstances) {
            System.out.println("increasing....");
            resizeInstanceGroup(++currSize);
        }
    }

    private static void decreaseInstanceGroup() throws IOException {
        currSize = computeService.instanceGroupManagers()
                .get(PROJECT_ID, ZONE_NAME, INSTANCE_GROUP).execute().getTargetSize();
        if(currSize > minInstances) {
            System.out.println("decreasing...");
            resizeInstanceGroup(--currSize);
        }
    }

    private static void resizeInstanceGroup(int newSize) throws IOException {
        Compute.InstanceGroupManagers.Resize request = computeService
                .instanceGroupManagers()
                .resize(PROJECT_ID, ZONE_NAME, INSTANCE_GROUP, newSize);
        request.execute();
    }

    private static void setLimits(int min, int max) {
        synchronized(mon) {
            minInstances = min;
            maxInstances = max;
        }
    }

    private static void setTarget(float tgt) {
        synchronized(mon) {
            target = tgt;
        }
    }

    @Override
    public void getMonitorState(Empty request, StreamObserver<MonitorState> responseObserver) {
        responseObserver.onNext(
                MonitorState.newBuilder()
                        .setCpuUsage(avg)
                        .setNInstances(currSize)
                        .build()
        );
        responseObserver.onCompleted();
    }

    @Override
    public void changeInstanceLimits(InstanceLimit request, StreamObserver<Empty> responseObserver) {
        setLimits(request.getMin(), request.getMax());
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    @Override
    public void changeTargetCpuUsage(TargetCpuUsage request, StreamObserver<Empty> responseObserver) {
        setTarget(request.getUsage());
        responseObserver.onNext(Empty.newBuilder().build());
        responseObserver.onCompleted();
    }

    private static class CpuPerc {
        private float perc;
        private long time;

        CpuPerc(float perc) {
            this.perc = perc;
            this.time = System.currentTimeMillis();
        }

        long getTime() {
            return time;
        }

        float getPerc() {
            return perc;
        }

        void setPerc(float perc) {
            this.time = System.currentTimeMillis();
            this.perc = perc;
        }
    }
}
