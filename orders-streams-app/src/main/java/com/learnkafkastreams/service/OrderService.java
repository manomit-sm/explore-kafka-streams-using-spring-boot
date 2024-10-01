package com.learnkafkastreams.service;

import com.learnkafkastreams.config.OrderServiceClient;
import com.learnkafkastreams.domain.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderService {

    private final OrderStoreService orderStoreService;
    private final MetaDataService metaDataService;
    private final OrderServiceClient orderServiceClient;

    @Value("${server.port}")
    private Integer port;

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {
        final ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);

        var spliterator = Spliterators.spliteratorUnknownSize(orderStore.all(), 0);
        var orderCountPerStoreDtoList = retrieveDataFromOtherInstances(orderType);
        return StreamSupport.stream(spliterator, false)
                .map(stringLongKeyValue -> new OrderCountPerStoreDTO(stringLongKeyValue.key, stringLongKeyValue.value))
                .collect(Collectors.toList());
    }

    private List<OrderCountPerStoreDTO> retrieveDataFromOtherInstances(String orderType) {
        final List<HostInfoDTO> hostInfoDTOS = otherHosts();
        if (!hostInfoDTOS.isEmpty()) {
            return otherHosts()
                    .stream().map(
                            hostInfoDTO -> orderServiceClient.retrieveOrdersCountsByHostPath(hostInfoDTO, orderType)
                    ).flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return null;
    }

    private List<HostInfoDTO> otherHosts() {
        // var currentMachineAddress = InetAddress.getLocalHost().getHostAddress();
        return metaDataService.getStreamsMetaData()
                .stream()
                .filter(hostInfoDTO -> hostInfoDTO.port() != port)
                .collect(Collectors.toList());
    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    public OrderCountPerStoreDTO getOrdersCountByLocation(String orderType, String locationId) {
        final ReadOnlyKeyValueStore<String, Long> orderCountStore = getOrderStore(orderType);
        var orderCount = orderCountStore.get(locationId);
        if (orderCount != null)
            return new OrderCountPerStoreDTO(locationId, orderCount);
        return null;
    }

    public List<AllOrdersCountPerStore> totalOrdersCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStore> mapper = (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStore(
                orderCountPerStoreDTO.locationId(),
                orderCountPerStoreDTO.orderCount(),
                orderType
        );

        var generalOrdersCount = getOrdersCount(GENERAL_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                .toList();

        var restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
                .toList();

        return Stream.of(generalOrdersCount, restaurantOrdersCount)
                .flatMap((Collection::stream))
                .toList();
    }

    public List<OrderRevenueDTO> revenueByOrderTYpe(String orderType) {

        var revenueStoreType = getRevenueStore(orderType);
        var revenueIterator = revenueStoreType.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);

        return StreamSupport
                .stream(spliterator, false)
                .map(stringLongKeyValue -> new OrderRevenueDTO(stringLongKeyValue.key, mapOrderType(orderType), stringLongKeyValue.value ) )
                .collect(Collectors.toList());
    }

    public static OrderType mapOrderType(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersRevenueStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Unexpected value: " + orderType);
        };
    }
}
