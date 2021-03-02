package com.quan12yt.kafkastream.controller;

import com.quan12yt.kafkastream.utils.Item;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class ProducerController {

    @Autowired
    KafkaTemplate<String, Item> kafkaTemplate;
    String leftTopic = "left-topic";
    String rightTopic = "right-topic";


    @PostMapping("/sendMess")
    public String sendMess(@RequestBody Item item){
        System.out.println("Processing --> " + item);
        kafkaTemplate.send(leftTopic, item.getId().toString(), item);
        return "publish successed";
    }

    @PostMapping("/sendMess2")
    public String sendMess2(@RequestBody Item item){
        System.out.println("Processing --> " + item);
        kafkaTemplate.send(rightTopic, item.getId().toString(), item);
        return "publish successed";
    }
}
