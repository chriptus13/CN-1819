import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import com.google.protobuf.Empty;
import forum.ExistingTopics;
import forum.ForumGrpc;
import forum.ForumMessage;
import forum.SubscribeUnSubscribe;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {
    private static final Scanner sc = new Scanner(System.in);
    private static final Pattern ptSub = Pattern.compile("/sub (\\w+)"),
            ptUnSub = Pattern.compile("/unsub (\\w+)"),
            ptPub = Pattern.compile("/pub (\\w+) (.+)"),
            ptPubBlob = Pattern.compile("/pub (\\w+) ([^;]*);([^\\s]+);([^\\s]+) ([^\\s]+)"),
            ptMessageBlob = Pattern.compile("([^;]*);([^\\s]+);([^\\s]+)"),
            ptTopics = Pattern.compile("/topics");

    private static final String svc_ip = "104.199.5.92";
    private static final int svc_port = 8000;
    private static ManagedChannel ch = ManagedChannelBuilder
            .forAddress(svc_ip, svc_port)
            .usePlaintext()
            .build();
    private static ForumGrpc.ForumStub stub = ForumGrpc.newStub(ch);
    private static StorageOptions options = StorageOptions.getDefaultInstance();
    private static Storage storage = options.getService();
    private static final Set<String> topics = new HashSet<>();

    public static void main(String[] args) {
        System.out.print("Username: ");
        String username = sc.next();
        sc.nextLine();
        do {
            String line = sc.nextLine();
            if (line.startsWith("/exit")) break;
            Matcher mSub = ptSub.matcher(line), mUnSub = ptUnSub.matcher(line),
                    mPubBlob = ptPubBlob.matcher(line), mPub = ptPub.matcher(line),
                    mTopics = ptTopics.matcher(line);
            if (mSub.matches())
                subscribe(username, mSub.group(1));
            else if (mUnSub.matches())
                unsubscribe(username, mUnSub.group(1));
            else if (mPubBlob.matches())
                publish(username, mPubBlob.group(1), mPubBlob.group(2), mPubBlob.group(3), mPubBlob.group(4), mPubBlob.group(5));
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
        System.out.println("/pub <topic> <text>[;<bucket>;<blob> <filename>] - publish message on topic");
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
                            if (user.equals(username)) return; // To avoid message to myself
                            String msg = forumMessage.getTxtMsg();
                            Matcher mMessageBlob = ptMessageBlob.matcher(msg);
                            if (mMessageBlob.matches())
                                downloadBlob(mMessageBlob.group(2), mMessageBlob.group(3));
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
            stub.topicUnSubscribe(
                    SubscribeUnSubscribe.newBuilder()
                            .setUsrName(username)
                            .setTopicName(topic)
                            .build(),
                    new StreamObserver<Empty>() {
                        public void onNext(Empty empty) {
                        }

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
                    public void onNext(Empty empty) {
                    }

                    public void onError(Throwable throwable) {
                        System.out.println("ERROR: " + throwable.getMessage());
                    }

                    public void onCompleted() {
                    }
                }
        );
    }

    // blobName must contain file extension
    private static void publish(String username, String topic, String msg, String bucketName, String blobName, String path) {
        try {
            Path uploadFrom = Paths.get(path);
            String contentType = Files.probeContentType(uploadFrom);
            BlobId blobId = BlobId.of(bucketName, blobName);
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
            stub.messagePublish(
                    ForumMessage.newBuilder()
                            .setFromUser(username)
                            .setTopicName(topic)
                            .setTxtMsg(msg + ";" + bucketName + ";" + blobName)
                            .build(),
                    new StreamObserver<Empty>() {
                        public void onNext(Empty empty) {
                        }

                        public void onError(Throwable throwable) {
                            System.out.println("ERROR: " + throwable.getMessage());
                        }

                        public void onCompleted() {
                        }
                    }
            );
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
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

    private static void downloadBlob(String bucketName, String blobName) {
        BlobId blobId = BlobId.of(bucketName, blobName);
        Blob blob = storage.get(blobId);
        try (PrintStream writeTo = new PrintStream(
                new FileOutputStream(new File(blobName)))) {
            if (blob.getSize() < 1_000_000) {
                // Blob is small read all its content in one request
                byte[] content = blob.getContent();
                writeTo.write(content);
            } else {
                // When Blob size is big use the blob's channel reader
                try (ReadChannel reader = blob.reader()) {
                    WritableByteChannel channel = Channels.newChannel(writeTo);
                    ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
                    while (reader.read(bytes) > 0) {
                        bytes.flip();
                        channel.write(bytes);
                        bytes.clear();
                    }
                }
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }

}
