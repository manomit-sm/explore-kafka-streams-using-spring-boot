package com.learnkafkastreams.topology;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.service.OrderService;
import com.learnkafkastreams.service.OrderWindowService;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;


@SpringBootTest
@EmbeddedKafka(topics = {ORDERS, STORES})
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class OrdersTopologyIntegrationTest {

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    OrderService orderService;

    @Autowired
    OrderWindowService orderWindowService;

    @BeforeEach
    void setup() {
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).start();
    }

    @AfterEach
    void tearDown() {
        Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams()).close();
        streamsBuilderFactoryBean.getKafkaStreams().cleanUp();
    }


    private void publishOrders() {
        orders()
                .forEach(order -> {
                    String orderJSON = null;
                    try {
                        orderJSON = objectMapper.writeValueAsString(order.value);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                    kafkaTemplate.send(ORDERS, order.key, orderJSON);
                });


    }


    static List<KeyValue<String, Order>> orders() {

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
                //LocalDateTime.now()
                LocalDateTime.parse("2023-02-21T21:25:01")
        );
        var keyValue1 = KeyValue.pair(order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair(order2.orderId().toString()
                , order2);


        return List.of(keyValue1, keyValue2);

    }

    @Test
    void ordersCount() throws UnknownHostException {
        publishOrders();

        //assert  orderService.getOrdersCount(GENERAL_ORDERS).size() == 1;

        Awaitility.await().atMost(10, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.getOrdersCount(GENERAL_ORDERS).size(), equalTo(1));

        var generalOrderCount = orderService.getOrdersCount(GENERAL_ORDERS);
        assertEquals(1, generalOrderCount.get(0).orderCount());
    }

    @Test
    void orderRevenue() {
        publishOrders();

        Awaitility.await().atMost(10, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderTYpe(GENERAL_ORDERS).size(), equalTo(1));

        Awaitility.await().atMost(10, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderService.revenueByOrderTYpe(RESTAURANT_ORDERS).size(), equalTo(1));

        var orderRevenueDto = orderService.revenueByOrderTYpe(GENERAL_ORDERS);

        assertEquals(new BigDecimal("27.00"), orderRevenueDto.get(0).totalRevenue().runningRevenue());
    }

    @Test
    void orderRevenueByWindow() {
        publishOrders();

        Awaitility.await().atMost(10, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderWindowService.getOrderRevenueByWindow(GENERAL_ORDERS).size(), equalTo(1));

        Awaitility.await().atMost(10, SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> orderWindowService.getOrderRevenueByWindow(RESTAURANT_ORDERS).size(), equalTo(1));

        var orderRevenueDtoByWindow = orderWindowService.getOrderRevenueByWindow(GENERAL_ORDERS);

        assertEquals(new BigDecimal("27.00"), orderRevenueDtoByWindow.get(0).totalRevenue().runningRevenue());
    }
}
