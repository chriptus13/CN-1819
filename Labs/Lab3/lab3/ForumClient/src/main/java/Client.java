import com.google.protobuf.Empty;
import forum.ExistingTopics;
import forum.ForumMessage;
import forum.ForumServiceGrpc;
import forum.SubscribeUnSubscribe;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {
    private static final Scanner sc = new Scanner(System.in);
    private static final Pattern ptSub = Pattern.compile("/sub (\\w+)"),
            ptUnSub = Pattern.compile("/unsub (\\w+)"),
            ptPub = Pattern.compile("/pub (\\w+) (.+)"),
            ptTopics = Pattern.compile("/topics");

    private static final String svc_ip = "localhost";
    private static final int svc_port = 7000;
    private static ManagedChannel ch = ManagedChannelBuilder
            .forAddress(svc_ip, svc_port)
            .usePlaintext()
            .build();
    private static ForumServiceGrpc.ForumServiceStub stub = ForumServiceGrpc.newStub(ch);
    private static final Set<String> topics = new HashSet<>();

    public static void main(String[] args) {
        System.out.print("Username: ");
        String username = sc.next();
        sc.nextLine();
        do {
            String line = sc.nextLine();
            if (line.startsWith("/exit")) break;
            Matcher mSub = ptSub.matcher(line), mUnSub = ptUnSub.matcher(line),
                    mPub = ptPub.matcher(line), mTopics = ptTopics.matcher(line);
            if (mSub.matches())
                subscribe(username, mSub.group(1));
            else if (mUnSub.matches())
                unsubscribe(username, mUnSub.group(1));
            else if (mPub.matches())
                publish(username, mPub.group(1), mPub.group(2));
            else if (mTopics.matches())
                topics();
            else if (line.startsWith("/help"))
                showHelp();
            else
                System.out.println("Invalid Command!");
        } while (true);
        exit(username);
    }

    private static void showHelp() {
        System.out.println("*** Help ***");
        System.out.println("/help - show commands");
        System.out.println("/sub <topic> - subscribe to topic");
        System.out.println("/unsub <topic> - unsubscribe to topic");
        System.out.println("/pub <topic> <message> - publish message on topic");
        System.out.println("/topics - show all topics");
        System.out.println("/exit - leave");
        System.out.println("*** **** ***");
    }

    private static void subscribe(String username, String topic) {
        if (!topics.contains(topic)) {
            topics.add(topic);
            stub.topicSubscribe(
                    SubscribeUnSubscribe.newBuilder()
                            .setUsrName(username)
                            .setTopicName(topic)
                            .build(),
                    new StreamObserver<ForumMessage>() {
                        public void onNext(ForumMessage forumMessage) {
                            String topicMsg = forumMessage.getTopicName();
                            String user = forumMessage.getFromUser();
                            String msg = forumMessage.getTxtMsg();
                            System.out.println(String.format("[%s|%s]: %s", topicMsg, user, msg));
                        }

                        public void onError(Throwable throwable) {
                            System.out.println("ERROR: " + throwable.getMessage());
                        }

                        public void onCompleted() {
                            System.out.println("UNSUBSCRIBED: " + topic);
                        }
                    }
            );
        }
    }

    private static void unsubscribe(String username, String topic) {
        if (topics.contains(topic))
            stub.topicUnsubscribe(
                    SubscribeUnSubscribe.newBuilder()
                            .setUsrName(username)
                            .setTopicName(topic)
                            .build(),
                    new StreamObserver<Empty>() {
                        public void onNext(Empty empty) { }

                        public void onError(Throwable throwable) {
                            System.out.println("ERROR: " + throwable.getMessage());
                        }

                        public void onCompleted() {
                            topics.remove(topic);
                        }
                    }
            );
    }

    private static void publish(String username, String topic, String msg) {
        stub.messagePublish(
                ForumMessage.newBuilder()
                        .setFromUser(username)
                        .setTopicName(topic)
                        .setTxtMsg(msg)
                        .build(),
                new StreamObserver<Empty>() {
                    public void onNext(Empty empty) { }

                    public void onError(Throwable throwable) {
                        System.out.println("ERROR: " + throwable.getMessage());
                    }

                    public void onCompleted() { }
                }
        );
    }

    private static void topics() {
        stub.getAllTopics(
                Empty.newBuilder().build(),
                new StreamObserver<ExistingTopics>() {
                    public void onNext(ExistingTopics existingTopics) {
                        System.out.println("*** TOPICS ***");
                        existingTopics.getTopicNameList()
                                .forEach(System.out::println);
                        System.out.println("*** ****** ***");
                    }

                    public void onError(Throwable throwable) {
                        System.out.println("ERROR: " + throwable.getMessage());
                    }

                    public void onCompleted() {
                    }
                }
        );
    }

    private static void exit(String username) {
        topics.forEach(topic -> unsubscribe(username, topic));
    }
}
