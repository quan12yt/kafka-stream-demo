package com.quan12yt.kafkastream.utils;

import com.quan12yt.kafkastream.utils.Item;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;


public class ItemSerdes extends Serdes.WrapperSerde<Item>{
    public ItemSerdes(){
        super(new JsonSerializer<>(), new JsonDeserializer<>(Item.class));
    }
}
