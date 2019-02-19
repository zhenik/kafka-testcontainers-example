package ru.zhenik.test.example.testcontainers;

import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.reset;

public class KafkaContainerExampleAvroIT {
  private static final String confluentPlatformVersion = "5.1.0";

  @ClassRule public static KafkaContainer kafka;

  @ClassRule public static GenericContainer schemaRegistry;

  @BeforeClass
  public static void atStart() {
    kafka = new KafkaContainer(confluentPlatformVersion);
    kafka.start();

    await()
        .pollDelay(5, TimeUnit.SECONDS)
        .ignoreExceptions()
        .until(
            () -> {
              System.out.println(kafka.getBootstrapServers());
              schemaRegistry =
                  new GenericContainer(
                          "confluentinc/cp-schema-registry:" + confluentPlatformVersion)
                      .withExposedPorts(8081)
                      .withNetwork(kafka.getNetwork())
                      .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                      .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                      .withEnv(
                          "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                          kafka.getBootstrapServers())
                      .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

              return schemaRegistry != null;
            });
  }

  @Test
  public void test(){
    System.out.println(kafka.getBootstrapServers());
  }


}
