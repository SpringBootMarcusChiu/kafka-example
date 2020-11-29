package com.marcuschiu.apachekafkaexample.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.web.bind.annotation.RestController;

@RestController
@SpringBootApplication
public class ApacheKafkaExampleConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(ApacheKafkaExampleConsumerApplication.class, args);
	}

//	@KafkaListener(topics = "topic1, topic2", groupId = "foo")
//	@KafkaListener(topics = "marcus-topic", groupId = "foo")
//	public void listenGroupFoo(String message) {
//		System.out.println("Received Message in group foo: " + message);
//	}

	@KafkaListener(topics = "topicName")
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received Message: " + message + " from partition: " + partition);
	}
}
