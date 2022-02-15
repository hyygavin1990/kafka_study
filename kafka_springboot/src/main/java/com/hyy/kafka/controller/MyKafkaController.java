package com.hyy.kafka.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/msg")
public class MyKafkaController {

    private static final String TOPIC_NAME = "my-replicated-topic";

    @Autowired
    private KafkaTemplate<String,String> kafkaTemplate;


    @RequestMapping("/send")
    public String send(){
        kafkaTemplate.send(TOPIC_NAME,0,"mykey","this is a message");
        return "ok";
    }
}
