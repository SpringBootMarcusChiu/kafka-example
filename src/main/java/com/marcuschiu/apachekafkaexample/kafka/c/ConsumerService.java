package com.marcuschiu.apachekafkaexample.kafka.c;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service("ConsumerServiceC")
public class ConsumerService {

    public static final String TOPIC = "c";

    @KafkaListener(
            groupId = "mock-group",
            topicPartitions = @TopicPartition(
                    topic = TOPIC,
                    partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}))
    public void exampleKafkaListener1(@Payload String message,
                                      @Header(KafkaHeaders.OFFSET) int offset) {
        System.out.println("Consumed message=['" + message + "'] from topic=['" + TOPIC + "'] from partition=['0'] on offset=['" + offset + "']");
    }

    @KafkaListener(
            groupId = "mock-group",
            topicPartitions = @TopicPartition(
                    topic = TOPIC,
                    partitionOffsets = {@PartitionOffset(partition = "1", initialOffset = "0")}))
    public void exampleKafkaListener2(@Payload String message,
                                      @Header(KafkaHeaders.OFFSET) int offset) {
        System.out.println("Consumed message=['" + message + "'] from topic=['" + TOPIC + "'] from partition=['1'] on offset=['" + offset + "']");
    }

    // for more @KafkaListener examples: https://reflectoring.io/spring-boot-kafka/
}
