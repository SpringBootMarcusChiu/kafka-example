package com.marcuschiu.apachekafkaexample.kafka.a;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController("ProducerControllerA")
public class ProducerController {

    private static final String TOPIC = "a";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/produce-message/a/{message}")
    public String sendMessage(@PathVariable(value = "message") String msg) {
        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(TOPIC, msg);
        future.addCallback(new ListenableFutureCallback<>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Produced message=['" + msg + "'] onto topic=['" + TOPIC + "'] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                System.out.println("Unable to send message=['" + msg + "'] due to : " + ex.getMessage());
            }
        });
        return "message sent";
    }
}
