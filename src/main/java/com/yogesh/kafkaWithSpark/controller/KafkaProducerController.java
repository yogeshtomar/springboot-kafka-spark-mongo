package com.yogesh.kafkaWithSpark.controller;

import com.yogesh.kafkaWithSpark.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/kafka")
public class KafkaProducerController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerController.class);

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @PostMapping("/publish")
    public String publishMessage(@RequestBody String message) {
        logger.info("Controller got message :{}", message);
        kafkaProducerService.sendMessage(message);
        return "Message sent to Kafka topic: " + message;
    }
}
