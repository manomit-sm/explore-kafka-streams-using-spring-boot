package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@Slf4j
public class OrdersTopology {

    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";
    public static final String STORES = "stores";



    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        orderTopology(streamsBuilder);

    }

    private static void orderTopology(StreamsBuilder streamsBuilder) {
        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Order.class))
                                .withTimestampExtractor(new OrderTimeStampExtractor())
                )
                .selectKey((key, value) -> value.locationId());

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), new JsonSerde<>(Store.class)));

        storesTable
                .toStream()
                .print(Printed.<String,Store>toSysOut().withLabel("stores"));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));


        orderStreams
                .split(Named.as("restaurant-stream"))
                .branch((key, order) -> order.orderType().equals(OrderType.GENERAL), Branched.withConsumer(generalStream -> {
                    /*generalStream
                            .mapValues((key, order) -> revenueValueMapper.apply(order))
                            .to(GENERAL_ORDERS, Produced.with(Serdes.String(), SerdeFactory.genericSerde(Revenue.class))); */
                    aggregateOrderByCount(generalStream, GENERAL_ORDERS_COUNT);
                    aggregateOrderByRevenue(generalStream, GENERAL_ORDERS_REVENUE, storesTable);
                    aggregateOrderByRevenueByTimeWindow(generalStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);
                }))
                .branch((key, order) -> order.orderType().equals(OrderType.RESTAURANT), Branched.withConsumer(restaurantStream -> {
                    /* restaurantStream
                            .mapValues((key, order) -> revenueValueMapper.apply(order))
                            .to(RESTAURANT_ORDERS, Produced.with(Serdes.String(), SerdeFactory.genericSerde(Revenue.class))) */
                    aggregateOrderByCount(restaurantStream, RESTAURANT_ORDERS_COUNT);
                    aggregateOrderByRevenue(restaurantStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                    aggregateOrderByRevenueByTimeWindow(restaurantStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);

                }));
    }

    private static void aggregateOrderByRevenueByTimeWindow(KStream<String, Order> generalStream, String storeName, KTable<String, Store> storeTable) {


        // final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(60));
        final TimeWindows timeWindows = TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(15));
        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator =
                (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order);

        final KTable<Windowed<String>, TotalRevenue> revenueKTable = generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(timeWindows)
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, WindowStore<Bytes, byte[]>>
                                        as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))

                );
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
        final KStream<String, TotalRevenueWithAddress> revenueWithStoreTable = revenueKTable
                .toStream()
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storeTable, valueJoiner, Joined.with(Serdes.String(), new JsonSerde<>(TotalRevenue.class), new JsonSerde<>(Store.class)));
        revenueWithStoreTable
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName + "byStore"));
    }

    private static void aggregateOrderByRevenue(KStream<String, Order> generalStream, String storeName, KTable<String, Store> storeTable) {



        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator =
                (key, order, aggregate) -> aggregate.updateRunningRevenue(key, order);

        final KTable<String, TotalRevenue> revenueKTable = generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .aggregate(
                        totalRevenueInitializer,
                        aggregator,
                        Materialized.<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>
                                        as(storeName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))

                );
        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
        final KTable<String, TotalRevenueWithAddress> revenueWithStoreTable = revenueKTable
                .join(storeTable, valueJoiner);
        revenueWithStoreTable
                .toStream()
                .print(Printed.<String, TotalRevenueWithAddress>toSysOut().withLabel(storeName + "byStore"));
    }

    private static void aggregateOrderByCount(KStream<String, Order> generalStream, String storeName) {

        final KTable<String, Long> ordersCountPerStore = generalStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))

                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .count(Named.as(storeName), Materialized.as(storeName));

        ordersCountPerStore.toStream()
                .print(Printed.<String, Long>toSysOut().withLabel(storeName));
    }
}
