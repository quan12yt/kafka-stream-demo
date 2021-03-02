package com.quan12yt.kafkastream.controller;

import com.quan12yt.kafkastream.utils.Item;
import com.quan12yt.kafkastream.utils.ItemSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.Properties;

@RestController
public class ProcessController {

    private KafkaStreams streams;

    private Properties properties() {
        Properties pro = new Properties();
        pro.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-id");
        pro.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return pro;
    }

    private void streamStart(StreamsBuilder builder) {
        if (streams != null) {
            streams.close();
        }
        final Topology topology = builder.build();
        streams = new KafkaStreams(topology, properties());
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @Autowired
    ProducerController producerController;

    @RequestMapping("/start/")
    public void startJoin() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Item> leftSource = builder.stream("left-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));
        KStream<String, Item> rightSource = builder.stream("right-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));

        KStream<String, Item> joined = leftSource
                .selectKey((key, value) -> key)
                .join(rightSource.selectKey((key, value) -> key)
                        , (value1, value2) -> {
                            System.out.println("value2.getName() >> " + value1.getName() + value2.getName());
                            value2.setCate(value1.getCate());
                            return value2;
                        }
                        , JoinWindows.of(Duration.ofSeconds(20))
                        , Joined.with(
                                Serdes.String(),
                                new ItemSerdes(),
                                new ItemSerdes()
                        )
                );
        joined.to("joined-topic", Produced.with(Serdes.String(), new ItemSerdes()));
        streamStart(builder);
    }

    @GetMapping("/countMessage")
    public void countMessage() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Item> stream = builder.stream("left-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));
        stream.groupByKey()
                .count()
                .toStream()
                .foreach((k, v) -> System.out.println("count: " + v));
        streamStart(builder);
    }
}
