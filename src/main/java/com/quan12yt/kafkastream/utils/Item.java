package com.quan12yt.kafkastream.utils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@NoArgsConstructor
@AllArgsConstructor
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
