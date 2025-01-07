package com.yogesh.kafkaWithSpark.controller;

import com.yogesh.kafkaWithSpark.service.KafkaProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaProducerController {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("/publish")
    public String publishMessage(@RequestBody String message) {
        System.out.println("Controller got message :" + message);
        kafkaProducerService.sendMessage(message);
        return "Message sent to Kafka topic: " + message;
    }
}
