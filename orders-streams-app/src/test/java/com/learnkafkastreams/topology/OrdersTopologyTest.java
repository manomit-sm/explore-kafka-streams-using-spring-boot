package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OrdersTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Order> ordersInputTopic = null;

    static String INPUT_TOPIC = ORDERS;

    OrdersTopology ordersTopology = new OrdersTopology();
    StreamsBuilder streamsBuilder = null;

    static List<KeyValue<String, Order>> orders(){

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                // LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );
        var keyValue1 = KeyValue.pair( order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair( order2.orderId().toString()
                , order2);


        return  List.of(keyValue1, keyValue2);

    }

    @BeforeEach
    void setUp() {
        streamsBuilder = new StreamsBuilder();
        ordersTopology.process(streamsBuilder);
        topologyTestDriver = new TopologyTestDriver(streamsBuilder.build());

        ordersInputTopic = topologyTestDriver.createInputTopic(
                ORDERS,
                Serdes.String().serializer(),
                new JsonSerde<>((Order.class)).serializer()
        );
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void ordersCount() {
        ordersInputTopic
                .pipeKeyValueList(orders());

        final KeyValueStore<String, Long> generalOrderCountStore = topologyTestDriver
                .getKeyValueStore(GENERAL_ORDERS_COUNT);

        final Long store1234 = generalOrderCountStore.get("store_1234");
        assertEquals(1, store1234);
    }

    @Test
    void ordersRevenue() {
        ordersInputTopic
                .pipeKeyValueList(orders());

        final KeyValueStore<String, TotalRevenue> generalOrderCountStore = topologyTestDriver
                .getKeyValueStore(GENERAL_ORDERS_REVENUE);

        final TotalRevenue store1234 = generalOrderCountStore.get("store_1234");
        assertEquals(1, store1234.runnuingOrderCount());
    }

    @Test
    void ordersRevenueMultiple() {
        ordersInputTopic
                .pipeKeyValueList(orders());
        ordersInputTopic
                .pipeKeyValueList(orders());

        final KeyValueStore<String, TotalRevenue> generalOrderCountStore = topologyTestDriver
                .getKeyValueStore(GENERAL_ORDERS_REVENUE);

        final TotalRevenue store1234 = generalOrderCountStore.get("store_1234");
        assertEquals(2, store1234.runnuingOrderCount());
    }

    @Test
    void ordersRevenueByWindow() {
        ordersInputTopic
                .pipeKeyValueList(orders());
        ordersInputTopic
                .pipeKeyValueList(orders());

        final WindowStore<String, TotalRevenue> generalOrderCountStore = topologyTestDriver
                .getWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);

        generalOrderCountStore
                .all()
                .forEachRemaining(windowedTotalRevenueKeyValue -> {
                    var startTime = windowedTotalRevenueKeyValue.key.window().startTime();
                    var endTime = windowedTotalRevenueKeyValue.key.window().endTime();
                    var totalRevenue = windowedTotalRevenueKeyValue.value;
                    assertEquals(2, totalRevenue.runnuingOrderCount());
                    var expectedStartTime = LocalDateTime.parse("2023-02-21T21:25:00");
                    var expectedEndTime = LocalDateTime.parse("2023-02-21T21:25:15");

                    assert LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))).equals(expectedStartTime);
                    assert LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST"))).equals(expectedEndTime);
                });
    }
}