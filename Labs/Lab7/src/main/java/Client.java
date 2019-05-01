import com.google.api.core.ApiFuture;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.*;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Client {
    private static final Scanner sc = new Scanner(System.in);
    private static final Pattern ptSub = Pattern.compile("/sub (\\w+)"),
            //ptUnSub = Pattern.compile("/unsub (\\w+)"),
            ptPub = Pattern.compile("/pub (\\w+) (.+)"),
            ptTopics = Pattern.compile("/topics"),
            ptCreateTopic = Pattern.compile("/ctopic (\\w+)"),
            ptSubs = Pattern.compile("/subs (\\w+)"),
            ptCreateSub = Pattern.compile("/csub (\\w+) (\\w+)");

    private static final String PROJECT_ID = ServiceOptions.getDefaultProjectId();
    private static final Executor poolExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    public static void main(String[] args) {
        showHelp();
        do {
            String line = sc.nextLine();
            if(line.startsWith("/exit")) break;
            Matcher mSub = ptSub.matcher(line),
                    //mUnSub = ptUnSub.matcher(line),
                    mPub = ptPub.matcher(line),
                    mTopics = ptTopics.matcher(line),
                    mCreateTopic = ptCreateTopic.matcher(line),
                    mSubs = ptSubs.matcher(line),
                    mCreateSub = ptCreateSub.matcher(line);
            if(mSub.matches())
                subscribe(mSub.group(1));
            else if(mPub.matches())
                publish(mPub.group(1), mPub.group(2));
            else if(mTopics.matches())
                showTopics();
            else if(mCreateTopic.matches())
                createTopic(mCreateTopic.group(1));
            else if(mSubs.matches())
                showTopicSubscriptions(mSubs.group(1));
            else if(mCreateSub.matches())
                createSubscription(mCreateSub.group(1), mCreateSub.group(2));
            /*else if(mUnSub.matches())
                unsubscribe(mUnSub.group(1));*/
            else if(line.startsWith("/help"))
                showHelp();
            else
                System.out.println("Invalid Command!");
        } while(true);
    }

    private static void createTopic(String name) {
        try(TopicAdminClient topicAdmin = TopicAdminClient.create()) {
            ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, name);
            topicAdmin.createTopic(topicName);
            System.out.println("Topic created!");
        } catch(IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void showHelp() {
        System.out.println("*** Help ***");
        System.out.println("/help - show commands");
        System.out.println("/topics - show all topics");
        System.out.println("/ctopic <topic> - create topic");
        System.out.println("/pub <topic> <text> - publish message on topic");
        System.out.println("/subs <topic> - show all subscriptions in topic");
        System.out.println("/csub <topic> <name> - create subscription in topic");
        System.out.println("/sub <name> - subscribe to the subscription");
        //System.out.println("/unsub <topic> - unsubscribe to topic");
        System.out.println("/exit - leave");
        System.out.println("*** **** ***");
    }

    private static void createSubscription(String topic, String subscription) {
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topic);
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, subscription);

        try(SubscriptionAdminClient clientAdmin = SubscriptionAdminClient.create()) {
            PushConfig pushConfig = PushConfig.getDefaultInstance();
            clientAdmin.createSubscription(subscriptionName, topicName, pushConfig, 0);
            System.out.println("Subscription created!");
        } catch(IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }

    private static void subscribe(String subscriptionName) {
        ProjectSubscriptionName pSubName = ProjectSubscriptionName.of(PROJECT_ID, subscriptionName);
        Subscriber subscriber = Subscriber.newBuilder(
                pSubName,
                (msg, ackReply) -> {
                    System.out.println("Message{" + msg.getMessageId() + "}:" + msg.getData().toStringUtf8());
                    ackReply.ack();
                }
        ).build();

        subscriber.startAsync().awaitRunning();
        System.out.println("Subscribed!");
    }

    private static void unsubscribe(String subscriptionName) {
        // TODO
    }

    private static void publish(String topic, String msg) {
        ProjectTopicName topicName = ProjectTopicName.of(PROJECT_ID, topic);
        try {
            Publisher publisher = Publisher.newBuilder(topicName).build();
            PubsubMessage pubsubMsg = PubsubMessage.newBuilder()
                    .setData(ByteString.copyFromUtf8(msg))
                    //metadata
                    .build();
            ApiFuture<String> fut = publisher.publish(pubsubMsg);

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

    private static void showTopicSubscriptions(String topic) {
        try(SubscriptionAdminClient clientAdmin = SubscriptionAdminClient.create()) {
            ProjectName projectName = ProjectName.of(PROJECT_ID);
            SubscriptionAdminClient.ListSubscriptionsPagedResponse subscriptions = clientAdmin.listSubscriptions(projectName);
            System.out.println("** Subscriptions for topic {" + topic + "} **");
            subscriptions.iterateAll().forEach(sub -> System.out.println("\t-> " + sub.getName()));
            System.out.println("** **");
        } catch(IOException e) {
            System.out.println("Error: " + e.getMessage());
        }

    }

    private static void showTopics() {
        try(TopicAdminClient topicAdmin = TopicAdminClient.create()) {
            TopicAdminClient.ListTopicsPagedResponse topics = topicAdmin.listTopics(ProjectName.of(PROJECT_ID));
            System.out.println("** Topics **");
            topics.iterateAll().forEach(topic -> System.out.println("\t-> " + topic.getName()));
            System.out.println("** ****** **");
        } catch(IOException e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
