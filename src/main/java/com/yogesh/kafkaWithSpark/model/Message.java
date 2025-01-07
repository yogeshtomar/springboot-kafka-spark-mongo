package com.yogesh.kafkaWithSpark.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;

@Getter @Setter
@Document(collation = "kafkaMessages")
public class Message implements Serializable {
    @Id
    private String id;
    private  String data;
    private String timestamp;
}
