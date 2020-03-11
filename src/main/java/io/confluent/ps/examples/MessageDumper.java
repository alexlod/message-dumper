package io.confluent.ps.examples;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class MessageDumper {
  private static final Logger log = LoggerFactory.getLogger(MessageDumper.class);

  private static Properties createConfig(String bootstrapServer) {
    Properties props = new Properties();

    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            org.apache.kafka.common.serialization.ByteArrayDeserializer.class);

    return props;
  }

  public static void main(String[] args) throws IOException {
    String bootstrapServer;
    String topic;
    int partition;
    long messageOffset;
    String outputFile;
    try {
      bootstrapServer = args[0];
      topic = args[1];
      partition = Integer.parseInt(args[2]);
      messageOffset = Long.parseLong(args[3]);
      outputFile = args[4];
    } catch (ArrayIndexOutOfBoundsException aioobe) {
      log.error("Couldn't parse arguments.", aioobe);
      log.error("Arguments must be: <bootstrap-server> <topic> <partition> <message-offset> <output-file>");
      log.error("For example: localhost:9092 my-topic 2 100100 /tmp/binary-message");
      throw new RuntimeException("Couldn't parse arguments.");
    }

    Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(createConfig(bootstrapServer));

    TopicPartition topicPartition = new TopicPartition(topic, partition);
    consumer.assign(Collections.singleton(topicPartition));
    consumer.seek(topicPartition, messageOffset);

    File keyFile = new File(outputFile + ".key");
    File valueFile = new File(outputFile + ".value");

    FileOutputStream keyStream = new FileOutputStream(keyFile);
    FileOutputStream valueStream = new FileOutputStream(valueFile);

    boolean found = false;
    while (! found) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Long.MAX_VALUE);
      for (ConsumerRecord<byte[], byte[]> record : records) {
        log.info("Dumping message with offset " + record.offset());
        keyStream.write(record.key());
        valueStream.write(record.value());
        found = true;
        break;
      }
    }

    log.info("Wrote binary key to " + keyFile.getAbsolutePath());
    log.info("Wrote binary value to " + valueFile.getAbsolutePath());

    keyStream.close();
    valueStream.close();
    consumer.close();
  }
}
