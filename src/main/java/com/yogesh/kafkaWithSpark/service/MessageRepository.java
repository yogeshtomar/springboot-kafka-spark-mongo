package com.yogesh.kafkaWithSpark.service;

import com.yogesh.kafkaWithSpark.model.Message;
import org.springframework.data.mongodb.repository.MongoRepository;

public interface MessageRepository extends MongoRepository<Message, String> {

}
