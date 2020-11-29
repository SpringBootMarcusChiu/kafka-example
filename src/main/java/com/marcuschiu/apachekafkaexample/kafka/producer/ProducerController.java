package com.marcuschiu.apachekafkaexample.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ProducerController {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Value(value = "${kafka.topic.name}")
    String topicName;

    @GetMapping("/produce-message/{message}")
    public void sendMessage(@PathVariable(value="message") String msg) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, msg);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Sent message=[" + msg + "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=[" + msg + "] due to : " + ex.getMessage());
            }
        });
    }
}
