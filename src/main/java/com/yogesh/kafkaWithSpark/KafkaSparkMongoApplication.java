package com.yogesh.kafkaWithSpark;

import com.yogesh.kafkaWithSpark.service.SparkKafkaStreamingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaSparkMongoApplication implements CommandLineRunner {
    @Autowired
    private SparkKafkaStreamingService sparkKafkaStreamingService;

    public static void main(String[] args) {
        SpringApplication.run(KafkaSparkMongoApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        sparkKafkaStreamingService.startSparkStream();
    }
}
