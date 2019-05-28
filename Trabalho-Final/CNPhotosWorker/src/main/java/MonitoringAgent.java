import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.hyperic.sigar.Sigar;

import java.net.InetAddress;

public class MonitoringAgent extends Thread {
    private final String projectId;
    private final String topicName;
    private final long interval;

    public MonitoringAgent(String projectId, String topicName, long interval) {
        this.projectId = projectId;
        this.topicName = topicName;
        this.interval = interval;
    }

    @Override
    public void run() {
        try {
            ProjectTopicName tName = ProjectTopicName.of(projectId, topicName);
            Publisher publisher = Publisher.newBuilder(tName).build();

            String hostName = InetAddress.getLocalHost().getHostName();
            Sigar sigar = new Sigar();
            while (true) {
                double perc = sigar.getCpuPerc().getCombined();
                String msg = String.format("%s:%.4f", hostName, perc);

                System.out.println(msg);
                ByteString msgData = ByteString.copyFromUtf8(msg);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder()
                        .setData(msgData)
                        .build();
                publisher.publish(pubsubMessage);

                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                    publisher.shutdown();
                    return;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
