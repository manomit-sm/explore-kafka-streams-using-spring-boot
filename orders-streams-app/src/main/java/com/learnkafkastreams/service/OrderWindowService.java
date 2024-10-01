package com.learnkafkastreams.service;


import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.*;
import static com.learnkafkastreams.topology.OrdersTopology.RESTAURANT_ORDERS_COUNT;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderWindowService {

    private final OrderStoreService orderStoreService;

    public List<OrdersCountPerStoreByWindowsDTO> getOrderByWindowCount(String orderType) {
        var countWindowStore = getCountWindowsStore(orderType);
        var orderTypeEnum = mapOrderType(orderType);
        final KeyValueIterator<Windowed<String>, Long> allIterator =
                countWindowStore.all();
        return getOrdersCountPerStoreByWindowsDTOS(allIterator, orderTypeEnum);

    }

    private static List<OrdersCountPerStoreByWindowsDTO> getOrdersCountPerStoreByWindowsDTOS(KeyValueIterator<Windowed<String>, Long> allIterator, OrderType orderTypeEnum) {
        var spliterator = Spliterators.spliteratorUnknownSize(allIterator, 0);
        return StreamSupport
                .stream(spliterator, false)
                .map(keyValue -> new OrdersCountPerStoreByWindowsDTO(
                        keyValue.key.key(),
                        keyValue.value,
                        orderTypeEnum,
                        LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                ))
                .collect(Collectors.toList());
    }

    private ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    private ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowsRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowsRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrderByWindowCountAll() {
        var generalOrderCountByWindow = getOrderByWindowCount(GENERAL_ORDERS);
        var restaurantOrderCountByWindow = getOrderByWindowCount(RESTAURANT_ORDERS);

        return getOrdersCountPerStoreByWindowsDTOS(generalOrderCountByWindow, restaurantOrderCountByWindow);
    }

    private static List<OrdersCountPerStoreByWindowsDTO> getOrdersCountPerStoreByWindowsDTOS(List<OrdersCountPerStoreByWindowsDTO> generalOrderCountByWindow, List<OrdersCountPerStoreByWindowsDTO> restaurantOrderCountByWindow) {
        return Stream.of(generalOrderCountByWindow, restaurantOrderCountByWindow)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrderByWindowCountAll(LocalDateTime fromTime, LocalDateTime toTime) {
        var generalOrderCountByWindows = getCountWindowsStore(GENERAL_ORDERS)
                //.backwardFetch() descending order
                .fetchAll(
                        fromTime.toInstant(ZoneOffset.UTC),
                        toTime.toInstant(ZoneOffset.UTC)
                );

        var generalOrderDto = getOrdersCountPerStoreByWindowsDTOS(generalOrderCountByWindows, OrderType.GENERAL);

        var restaurantOrderCountByWindows = getCountWindowsStore(RESTAURANT_ORDERS)
                .fetchAll(
                        fromTime.toInstant(ZoneOffset.UTC),
                        toTime.toInstant(ZoneOffset.UTC)
                );
        var restaurantOrderDto = getOrdersCountPerStoreByWindowsDTOS(restaurantOrderCountByWindows, OrderType.RESTAURANT);

        return getOrdersCountPerStoreByWindowsDTOS(generalOrderDto, restaurantOrderDto);
    }

    public List<OrdersRevenuePerStoreByWindowsDTO> getOrderRevenueByWindow(String orderType) {
        var revenueWindowStore = getRevenueWindowsStore(orderType);
        var orderTypeEnum = mapOrderType(orderType);
        var revenueWindowIterator = revenueWindowStore.all();

        var spliterator = Spliterators.spliteratorUnknownSize(revenueWindowIterator, 0);
        return StreamSupport
                .stream(spliterator, false)
                .map(keyValue -> new OrdersRevenuePerStoreByWindowsDTO(
                        keyValue.key.key(),
                        keyValue.value,
                        orderTypeEnum,
                        LocalDateTime.ofInstant(keyValue.key.window().startTime(), ZoneId.of("GMT")),
                        LocalDateTime.ofInstant(keyValue.key.window().endTime(), ZoneId.of("GMT"))
                ))
                .collect(Collectors.toList());
    }
}
