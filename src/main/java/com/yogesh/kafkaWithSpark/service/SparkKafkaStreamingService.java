package com.yogesh.kafkaWithSpark.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.yogesh.kafkaWithSpark.config.KafkaConfig;
import com.yogesh.kafkaWithSpark.model.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class SparkKafkaStreamingService {
    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private MessageRepository messageRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public void startSparkStream() {
        System.out.println("Starting Kafka Consumer service and Apache Spark");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Kafka-Spark-MongoDB")
                .setMaster("local[*]");


        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));


        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Collections.singletonList("test-topic"), kafkaConfig.getKafkaParams()));

        stream.foreachRDD(rdd -> {

            rdd.foreach(message -> {
                // Process the message (e.g., save to MongoDB)
                System.out.println("--Received message: " + message.value());
            });
        });


        stream.foreachRDD(this::processAndSaveMessages);



        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void processAndSaveMessages(JavaRDD<ConsumerRecord<String, String>> rdd) {
        // Extract only the serializable fields (e.g., message value)
        JavaRDD<String> messages = rdd.map(ConsumerRecord::value);

        // Collect messages to the driver and process
        List<Message> processedMessages = messages.collect().stream()
                .map(this::convertToMessage)
                .filter(msg -> msg != null)
                .collect(Collectors.toList());

        if (!processedMessages.isEmpty()) {
            messageRepository.saveAll(processedMessages);
            System.out.println("Saved " + processedMessages.size() + " messages to MongoDB");
        }
    }

    private Message convertToMessage(String jsonMessage) {
        try {
            JsonNode jsonNode = objectMapper.readTree(jsonMessage);
            String id = jsonNode.get("id").asText();
            String data = jsonNode.get("data").asText();
            Message msg = new Message();
            msg.setId(id);
            msg.setData(data);
            msg.setTimestamp(new Date().toString());
            return msg;
        } catch (Exception e) {
            System.err.println("Failed to parse message: " + jsonMessage);
            e.printStackTrace();
            return null;
        }
    }
}
