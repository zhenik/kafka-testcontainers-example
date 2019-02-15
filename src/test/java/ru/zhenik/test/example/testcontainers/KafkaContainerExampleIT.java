package ru.zhenik.test.example.testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaContainerExampleIT {

  private final String confluentPlatformVersion = "5.1.0";

  @Rule public KafkaContainer kafka = new KafkaContainer(confluentPlatformVersion);

  @Test
  public void adminApi_createTopic()
      throws InterruptedException, ExecutionException, TimeoutException {
    // Arrange
    final AdminClient adminClient =
        KafkaAdminClient.create(Utils.defaultPropertiesAdmin(kafka.getBootstrapServers()));

    // Act
    createTopic(adminClient, "new-topic", 5, (short) 1);

    // Assert
    final Collection<TopicListing> topicListings =
        adminClient.listTopics().listings().get(5, TimeUnit.SECONDS);

    assertTrue(
        topicListings.stream()
            .filter(topicListing -> !topicListing.isInternal())
            .anyMatch(topic -> "new-topic".equals(topic.name())));
  }

  @Test
  public void producerConsumer_sendReceive() throws ExecutionException, InterruptedException {
    // Arrange
    final String topic = "topic-a";

    final KafkaProducer<String, String> stringStringKafkaProducer =
        new KafkaProducer<>(Utils.defaultPropertiesProducer(kafka.getBootstrapServers()));

    final KafkaConsumer<String, String> stringStringKafkaConsumer =
        new KafkaConsumer<>(
            Utils.defaultPropertiesConsumer(
                kafka.getBootstrapServers(), "consumer-group-" + Instant.now().getEpochSecond()));

    // Act
    // 1 send async, with callBack
    stringStringKafkaProducer.send(
        new ProducerRecord<>(topic, "some-value-1"),
        ((metadata, exception) -> {
          if (exception == null) {
            Map<String, Object> data = new HashMap<>();
            data.put("topic", metadata.topic());
            data.put("partition", metadata.partition());
            data.put("offset", metadata.offset());
            data.put("timestamp", metadata.timestamp());
            System.out.println(data);
          } else {
            fail();
          }
        }));

    // 2 send async, without call back
    stringStringKafkaProducer.send(new ProducerRecord<>(topic, "some-value-2"));
    // 3 send sync way
    stringStringKafkaProducer.send(new ProducerRecord<>(topic, "some-value-3")).get();

    // Assert
    stringStringKafkaConsumer.subscribe(Collections.singleton(topic));
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              final ConsumerRecords<String, String> records =
                  stringStringKafkaConsumer.poll(Duration.ofSeconds(4));
              return records.count() == 3;
            });
  }

  @Test
  public void streamApp_anagram() {
    // Arrange
    final String topicInput = "topic-input";
    final String topicResult = "topic-result";
    final KafkaProducer<String, String> stringStringKafkaProducer =
        new KafkaProducer<>(Utils.defaultPropertiesProducer(kafka.getBootstrapServers()));

    final StreamProcessing streamProcessing =
        new StreamProcessing(kafka.getBootstrapServers(), topicInput, topicResult);

    final Properties consumerProperties =
        Utils.defaultPropertiesConsumer(
            kafka.getBootstrapServers(), "consumer-group-" + Instant.now().getEpochSecond());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
    final KafkaConsumer<String, Long> stringLongKafkaConsumer =
        new KafkaConsumer<>(consumerProperties);

    // Act
    // 1. send 3 anagrams, 1 not anagram
    // 2. run streams
    stringStringKafkaProducer.send(new ProducerRecord<>(topicInput, "magic"));
    stringStringKafkaProducer.send(new ProducerRecord<>(topicInput, "gamic"));
    stringStringKafkaProducer.send(new ProducerRecord<>(topicInput, "cimag"));
    stringStringKafkaProducer.send(new ProducerRecord<>(topicInput, "ignored"));

    streamProcessing.run();
    // good practise
    Runtime.getRuntime().addShutdownHook(new Thread(streamProcessing::stopStreams));

    await().pollDelay(3, TimeUnit.SECONDS).until(()->{
        streamProcessing.stopStreams();
        return true;
    });



    // Assert
    final String sortedCharsAnagram = "acgim";
    stringLongKafkaConsumer.subscribe(Collections.singleton(topicResult));
    await()
        .atMost(10, TimeUnit.SECONDS)
        .until(
            () -> {
              final ConsumerRecords<String, Long> records =
                  stringLongKafkaConsumer.poll(Duration.ofSeconds(4));

              return Utils.asStream(records.iterator())
                  .peek(r -> System.out.println("Here: "+ r.key() + ":" + r.value()))
                  .anyMatch(
                      record -> record.key().equals(sortedCharsAnagram) && record.value() == 3);
            });
  }

  /**
   * If amount of brokers less than given replication factor, than while topic creation given
   * replication will be ignored and set to 1
   */
  public static void createTopic(
      final AdminClient adminClient,
      final String topicName,
      final int partitions,
      final short replicationFactor) {

    try {
      // Define topic
      final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

      // Create topic, which is async call.
      final CreateTopicsResult createTopicsResult =
          adminClient.createTopics(Collections.singleton(newTopic));

      // Since the call is Async, Lets wait for it to complete.
      createTopicsResult.values().get(topicName).get();
    } catch (InterruptedException | ExecutionException e) {

      if (!(e.getCause() instanceof TopicExistsException)) {
        throw new RuntimeException(e.getMessage(), e);
      }

      // TopicExistsException - Swallow this exception, just means the topic already exists.
      System.out.println("Topic : " + topicName + " already exists");
    }
  }
}
