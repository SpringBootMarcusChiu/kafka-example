package com.marcuschiu.apachekafkaexample.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaTopicsConfiguration {

    private String topicName = "mock-topic-name";

    @Bean
    public NewTopic topic1() {
        return new NewTopic(topicName, 1, (short) 1);
    }
}
