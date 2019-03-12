package ru.zhenik.test.example.testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import ru.zhenik.test.example.schema.avro.v1.Word;

import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KafkaContainerExampleAvroIT {
  private static final String confluentPlatformVersion = "5.1.0";

  @ClassRule public static KafkaContainer kafka;

  @ClassRule public static GenericContainer schemaRegistry;

  @BeforeClass
  public static void atStart() {
    kafka = new KafkaContainer(confluentPlatformVersion);
    kafka.start();

    schemaRegistry =
        new GenericContainer(
            "confluentinc/cp-schema-registry:" + confluentPlatformVersion)
            .withExposedPorts(8081)
            .withNetwork(kafka.getNetwork())
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                kafka.getBootstrapServers())
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .withNetwork(Network.SHARED);

    schemaRegistry.start();
  }

  @Test
  public void testSchemaStarted() {
    final boolean running = schemaRegistry.isRunning();

    System.out.println("SCHEMA_CONTAINER_ID: "+ schemaRegistry.getContainerId());
    System.out.println("SCHEMA_CONTAINER_ID: "+ schemaRegistry.getContainerInfo());
    assertTrue(running);
  }

  @Test
  public void producerConsumer_sendReceive() throws ExecutionException, InterruptedException {
    final String topic = "word-topic-"+Instant.now().getEpochSecond();

    // schema registry: ip->docker-machine ip, port->mapping are dynamic
    final String schemaUrl = String.format("http://%s:%03d", schemaRegistry.getContainerIpAddress(), schemaRegistry.getFirstMappedPort());

    final Properties producerProperties = Utils.avroPropertiesProducer(kafka.getBootstrapServers(), schemaUrl);
    final Properties consumerProperties = Utils.avroPropertiesConsumer("consumer-group" + Instant.now().getEpochSecond(), kafka.getBootstrapServers(), schemaUrl);
    final KafkaProducer<String, Word> wordProducer = new KafkaProducer<>(producerProperties);
    final KafkaConsumer<String, Word> wordConsumer = new KafkaConsumer<String, Word>(consumerProperties);

    //Act

    // 1 send async, with callBack
    wordProducer.send(
        new ProducerRecord<>(
            topic,
            Word.newBuilder()
                .setPayload("some-payload-1")
                .build()),
        ((metadata, exception) -> {
          if (exception == null) {
            Map<String, Object> data = new HashMap<>();
            data.put("topic", metadata.topic());
            data.put("partition", metadata.partition());
            data.put("offset", metadata.offset());
            data.put("timestamp", metadata.timestamp());
            System.out.println("confirmation with metadata: "+data);
          } else {
            fail();
          }
        }));

    // 2 send async, without call back
    wordProducer.send(new ProducerRecord<>(
        topic,
        Word.newBuilder()
            .setPayload("some-payload-2")
            .build()));

    // 3 send sync way
    wordProducer.send(new ProducerRecord<>(topic,
        Word.newBuilder()
            .setPayload("some-payload-3")
            .build()))
        .get();


    // Assert
    wordConsumer.subscribe(Collections.singleton(topic));
    await()
        .atMost(5, TimeUnit.SECONDS)
        .until(
            () -> {
              final ConsumerRecords<String, Word> records =
                  wordConsumer.poll(Duration.ofSeconds(4));
              return records.count() == 3;
            });
  }


}
