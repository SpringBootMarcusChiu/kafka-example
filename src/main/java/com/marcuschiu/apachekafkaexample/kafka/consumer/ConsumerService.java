package com.marcuschiu.apachekafkaexample.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics = "marcus-topic")
    public void listenWithHeaders(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                                  @Header(KafkaHeaders.OFFSET) int offset) {
        System.out.println("Consumed Message: `" + message + "` from partition: " + partition + " and offset: " + offset);
    }

    @KafkaListener(groupId = "reflectoring-group-3",
            topicPartitions = @TopicPartition(
                    topic = "reflectoring-1",
                    partitionOffsets = { @PartitionOffset(partition = "0", initialOffset = "0") }))
    public void exampleKafkaListener(@Payload String message) {
        System.out.println("Received message [" + message + "]");
    }

    // for more @KafkaListener examples: https://reflectoring.io/spring-boot-kafka/
}
