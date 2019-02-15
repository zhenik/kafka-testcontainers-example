package ru.zhenik.test.example.testcontainers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Instant;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

public class Utils {

  static Properties defaultPropertiesProducer(final String bootstrapServers) {
    final Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-words-example-id1");
    properties.put(ProducerConfig.ACKS_CONFIG, "all");
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.RETRIES_CONFIG, 0);
    return properties;
  }

  static Properties defaultPropertiesConsumer(final String bootstrapServers, final String consumerGroup) {
    final Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // last committed offset by consumer group is stored on kafka side
    // make sure that tests are idempotent
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-"+Instant.now().getEpochSecond());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    return properties;
  }

  static Properties defaultPropertiesAdmin(final String bootstrapServers) {
    final Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin-client-id1" + Instant.now().getEpochSecond());
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 500);
    return properties;
  }

  static Properties defaultPropertiesStream(final String bootstrapServers) {
    final Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-app-id-" + Instant.now().getEpochSecond());
    properties.put(StreamsConfig.CLIENT_ID_CONFIG, "stream-client-id-" + Instant.now().getEpochSecond());
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return properties;
  }

  static Properties avroPropertiesProducer(final String bootstrapServers, final String schemaRegistryUrl) {
    final Properties properties = defaultPropertiesProducer(bootstrapServers);
    // override serializers
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    return withSchemaRegistry(properties, schemaRegistryUrl);
  }

  static Properties avroPropertiesConsumer(final String consumerGroup, final String bootstrapServers, final String schemaRegistryUrl) {
    final Properties properties = defaultPropertiesConsumer(bootstrapServers, consumerGroup);
    // override deserializers
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    return withSchemaRegistry(properties, schemaRegistryUrl);
  }

  private static Properties withSchemaRegistry(final Properties properties, final String schemaRegistryUrl) {
    properties.put("schema.registry.url", schemaRegistryUrl);
    return properties;
  }

  public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
    return asStream(sourceIterator, false);
  }

  public static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
    Iterable<T> iterable = () -> sourceIterator;
    return StreamSupport.stream(iterable.spliterator(), parallel);
  }

}
