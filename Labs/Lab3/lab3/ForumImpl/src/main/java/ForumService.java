import com.google.protobuf.Empty;
import forum.ExistingTopics;
import forum.ForumMessage;
import forum.ForumServiceGrpc;
import forum.SubscribeUnSubscribe;
import io.grpc.*;
import io.grpc.stub.StreamObserver;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@ThreadSafe
public class ForumService extends ForumServiceGrpc.ForumServiceImplBase {
    private final Lock monitor = new ReentrantLock();
    private final Map<String, Map<String, StreamObserver<ForumMessage>>> map = new HashMap<>();

    private static final int svcPort = 7000;

    public static void main(String[] args) throws IOException {
        Server svc = ServerBuilder
                .forPort(svcPort)
                .addService(new ForumService())
                .build()
                .start();
        System.out.println("Server started, listening on " + svcPort);

        System.out.print("Press any key to terminate...");
        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();

        svc.shutdown();
    }

    @Override
    public void topicSubscribe(SubscribeUnSubscribe request, StreamObserver<ForumMessage> responseObserver) {
        try {
            monitor.lock();
            String username = request.getUsrName();
            String topic = request.getTopicName();
            Map<String, StreamObserver<ForumMessage>> consumers =
                    map.computeIfAbsent(topic, str -> new HashMap<>());
            consumers.put(username, responseObserver);
        } finally {
            monitor.unlock();
        }
    }

    @Override
    public void topicUnsubscribe(SubscribeUnSubscribe request, StreamObserver<Empty> responseObserver) {
        try {
            monitor.lock();
            String username = request.getUsrName();
            String topic = request.getTopicName();
            Map<String, StreamObserver<ForumMessage>> consumers = map.get(topic);
            if(consumers != null) {
                StreamObserver<ForumMessage> removedStream = consumers.remove(username);
                if(removedStream != null) removedStream.onCompleted();
            }
            responseObserver.onNext(Empty.newBuilder().build()); // To avoid exception
            responseObserver.onCompleted();
        } finally {
            monitor.unlock();
        }
    }

    @Override
    public void getAllTopics(Empty request, StreamObserver<ExistingTopics> responseObserver) {
        try {
            monitor.lock();
            responseObserver.onNext(
                    ExistingTopics.newBuilder()
                            .addAllTopicName(map.keySet())
                            .build()
            );
            responseObserver.onCompleted();
        } finally {
            monitor.unlock();
        }
    }

    @Override
    public void messagePublish(ForumMessage request, StreamObserver<Empty> responseObserver) {
        try {
            monitor.lock();
            String topic = request.getTopicName();
            String username = request.getFromUser();
            Map<String, StreamObserver<ForumMessage>> consumers = map.get(topic);
            if(consumers != null)
                consumers.entrySet().stream()
                        .filter(p -> !p.getKey().equals(username))
                        .forEach(p -> {
                            try {
                                p.getValue().onNext(request);
                            } catch(StatusRuntimeException ex) {
                                System.out.println("Removing user [" + p.getKey() + "] invalid connection");
                                consumers.remove(p.getKey());
                            }
                        });
            else responseObserver.onError(
                    Status.FAILED_PRECONDITION
                            .withDescription("No such topic!")
                            .asException()
            );
            responseObserver.onNext(Empty.newBuilder().build()); // To avoid exception
            responseObserver.onCompleted();
        } finally {
            monitor.unlock();
        }
    }
}
