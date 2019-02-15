package ru.zhenik.test.example.testcontainers;

import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class StreamProcessing implements Runnable {
  private final String bootstrapServers;
  private final KafkaStreams kafkaStreams;

  public StreamProcessing(final String bootstrapServers, final String input, final String output) {
    this.bootstrapServers = bootstrapServers;
    this.kafkaStreams = new KafkaStreams(buildStreamTopology(input, output), Utils.defaultPropertiesStream(bootstrapServers));
  }


  public Topology buildStreamTopology(final String topic, final String resultTopic) {
    final StreamsBuilder streamsBuilder = new StreamsBuilder();
    final KStream<String, String>
        sourceStream = streamsBuilder.stream(topic, Consumed.with(Serdes.String(), Serdes.String()));

    // 1. [null:"magic"] => ["acgim":"magic"]
    // 2. amount with same key
    sourceStream
        .map((key, value)-> {
          final String newKey = Stream
              .of(value.replaceAll(" ", "").split(""))
              .sorted()
              .collect(Collectors.joining());

          return KeyValue.pair(newKey, value);
        })
        .groupByKey()
        .count()
        .toStream()
        .to(resultTopic, Produced.with(Serdes.String(), Serdes.Long()));

    return streamsBuilder.build();
  }

  @Override public void run() { kafkaStreams.start(); }

  void stopStreams() { Optional.ofNullable(kafkaStreams).ifPresent(KafkaStreams::close); }
}
