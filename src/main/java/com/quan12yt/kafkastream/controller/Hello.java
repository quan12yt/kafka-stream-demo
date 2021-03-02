//package com.quan12yt.kafkastream;
//
//import lombok.AllArgsConstructor;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serdes;
//import org.apache.kafka.common.utils.Bytes;
//import org.apache.kafka.streams.KafkaStreams;
//import org.apache.kafka.streams.StreamsBuilder;
//import org.apache.kafka.streams.StreamsConfig;
//import org.apache.kafka.streams.kstream.*;
//import org.apache.kafka.streams.processor.TopicNameExtractor;
//import org.apache.kafka.streams.state.KeyValueStore;
//import org.springframework.boot.context.properties.bind.Name;
//import org.springframework.context.annotation.Bean;
//import org.springframework.stereotype.Component;
//
//import java.util.Arrays;
//import java.util.Locale;
//import java.util.Properties;
//import java.util.Set;
//import java.util.regex.Pattern;
//
//@Component
//public class Hello {
//
//    private static final String topic = "inputTopic";
//
//    @Bean
//    public void helloword() throws InterruptedException {
//        Properties pro = new Properties();
//        pro.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-hello");
//        pro.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        pro.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//        pro.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//
//        StreamsBuilder builder = new StreamsBuilder();
//        KStream<String, String> line = builder.stream(topic);
//        KStream<String, Long> out = builder.stream("outputTopic");
//        KStream<String, Long> finalT = builder.stream("finalTopic");
//        line.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.ROOT).split("\\W+")))
//                .groupBy((key, value) -> value)
//                .count(Named.as("counts-store"))
//                .toStream()
//                .to("outputTopic", Produced.with(Serdes.String(), Serdes.Long()));
//
//        out.to("finalTopic", Produced.with(Serdes.String(), Serdes.Long()));
//        finalT.foreach((w, c) -> System.out.println("word: " + w + "-- " + c));
//
//        KafkaStreams streams = new KafkaStreams(builder.build(), pro);
//        streams.start();
//
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
//    }
//}
