package com.quan12yt.kafkastream.controller;

import com.quan12yt.kafkastream.utils.Item;
import com.quan12yt.kafkastream.utils.ItemSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
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
        streams.cleanUp();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @Autowired
    ProducerController producerController;

    @RequestMapping("/join")
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
                            System.out.println(" updated quantity >> " + value2.getQuantity());
                            value2.setQuantity(value1.getQuantity() + value2.getQuantity());
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

    @GetMapping("/merge")
    public void merge() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Item> leftSource = builder.stream("left-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));
        KStream<String, Item> rightSource = builder.stream("right-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));

        KStream<String, Item> merge = leftSource.merge(rightSource);
        merge = merge.flatMap((k, v) -> {
            List<KeyValue<String, Item>> result = new LinkedList<>();
            result.add(KeyValue.pair(k + "3", v ));
            result.add(KeyValue.pair(k + "2", v ));
            return result;
        });
        merge.to("merge-topic", Produced.with(Serdes.String(), new ItemSerdes()));
        merge.print(Printed.toSysOut());
        streamStart(builder);
    }

    @GetMapping("/global")
    public void global() {
        final StreamsBuilder builder = new StreamsBuilder();
        GlobalKTable<String, Item> table = builder.globalTable("left-topic", Materialized.<String, Item, KeyValueStore<Bytes, byte[]>>as(
                "global-store" /* table/store name */)
                .withKeySerde(Serdes.String()) /* key serde */
                .withValueSerde(new ItemSerdes()) /* value serde */
        );
        streamStart(builder);
        ReadOnlyKeyValueStore view = streams.store("global-store", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, Item> lst = view.all();
        while (lst.hasNext()){
            System.out.println(lst.next());
        }
    }

    @GetMapping("/map")
    public void map() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Item> leftSource = builder.stream("left-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));
        KStream<String, Item> rightSource = builder.stream("right-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));

        KStream<String, Item> merge = leftSource.merge(rightSource);
        merge = merge.map((k, v) -> KeyValue.pair(k, new Item(v.getId(), "new Item", v.getQuantity())));
        merge.to("merge-topic", Produced.with(Serdes.String(), new ItemSerdes()));
        merge.print(Printed.toSysOut());
        streamStart(builder);
    }

    @GetMapping("/filter")
    public void filter() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Item> leftSource = builder.stream("left-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));
        leftSource = leftSource.filter((k,v) -> k.equals("6"));
        leftSource.to("right-topic", Produced.with(Serdes.String(), new ItemSerdes()));
        leftSource.print(Printed.toSysOut());
        streamStart(builder);
    }

    @GetMapping("/count")
    public void count() {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, Item> leftSource = builder.stream("left-topic"
                , Consumed.with(Serdes.String(), new ItemSerdes()));
        KTable<String, Long> table = leftSource.selectKey((k, v) -> k).groupByKey().count();
        table.toStream().to("count-topic", Produced.with(Serdes.String(), Serdes.Long()));
        streamStart(builder);

    }
}
