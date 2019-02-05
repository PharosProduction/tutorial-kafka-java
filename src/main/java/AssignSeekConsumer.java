import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class AssignSeekConsumer {

  public static void main(String[] args) {
    String server = "127.0.0.1:9092";
    String topic = "user_registered";
    long offset = 15L;
    int partitionNum = 0;
    int numOfMessages = 5;

    new AssignSeekConsumer(server, topic).run(offset, partitionNum, numOfMessages);
  }

  // Variables

  private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
  private final String mBootstrapServer;
  private final String mTopic;

  // Constructor

  private AssignSeekConsumer(String bootstrapServer, String topic) {
    mBootstrapServer = bootstrapServer;
    mTopic = topic;
  }

  // Public

  void run(long offset, int partitionNum, int numOfMessages) {
    Properties props = consumerProps(mBootstrapServer);
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    setupConsumer(consumer, offset, partitionNum);
    fetchMessages(consumer, numOfMessages);
  }

  // Private

  private void setupConsumer(KafkaConsumer<String, String> consumer, long offset, int partitionNum) {
    TopicPartition partition = new TopicPartition(mTopic, partitionNum);
    consumer.assign(Collections.singletonList(partition));
    consumer.seek(partition, offset);
  }

  private void fetchMessages(KafkaConsumer<String, String> consumer, int numOfMessages) {
    int numberOfMessagesRead = 0;
    boolean keepOnReading = true;

    while (keepOnReading) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      for (ConsumerRecord<String, String> record : records) {
        numberOfMessagesRead += 1;

        mLogger.info("Key: " + record.key() + ", Value: " + record.value());
        mLogger.info("Partition: " + record.partition() +  ", Offset: " + record.offset());

        if (numberOfMessagesRead >= numOfMessages) {
          keepOnReading = false;
          break;
        }
      }
    }
  }

  private Properties consumerProps(String bootstrapServer) {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    return properties;
  }
}
