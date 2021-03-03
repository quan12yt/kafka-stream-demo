package com.quan12yt.kafkastream.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
public class Item {

    private Integer id;
    private String name;
    private Long quantity;

    @JsonCreator
    public Item(@JsonProperty("id") int id, @JsonProperty("name") String name, @JsonProperty("quantity") Long quantity) {
        this.id = id;
        this.name = name;
        this.quantity = quantity;
    }

}
