package com.marcuschiu.apachekafkaexample.kafka.a;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service("ConsumerServiceA")
public class ConsumerService {

    private static final String TOPIC = "a";

    @KafkaListener(topics = TOPIC)
    public void listenWithHeaders(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                                  @Header(KafkaHeaders.OFFSET) int offset) {
        System.out.println("Consumed message=['" + message + "'] from topic=['" + TOPIC + "'] from partition=['" + partition + "'] on offset=['" + offset + "']");
    }

    @KafkaListener(topics = TOPIC,
            containerFactory = "kafkaListenerContainerFactory")
    public void listenWithHeaders2(@Payload String message,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                                  @Header(KafkaHeaders.OFFSET) int offset) {
        System.out.println("Consumed message=['" + message + "'] from topic=['" + TOPIC + "'] from partition=['" + partition + "'] on offset=['" + offset + "']");
    }

    @KafkaListener(topics = TOPIC,
            groupId = "mock-group")
    public void exampleKafkaListener(@Payload String message,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
                                     @Header(KafkaHeaders.OFFSET) int offset) {
        System.out.println("Consumed message=['" + message + "'] from topic=['" + TOPIC + "'] from partition=['" + partition + "'] on offset=['" + offset + "']");
    }

    // for more @KafkaListener examples: https://reflectoring.io/spring-boot-kafka/
}
