package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.TotalRevenue;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.Objects;

@Service
@RequiredArgsConstructor
public class OrderStoreService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    public ReadOnlyKeyValueStore<String, Long> ordersCountStore(String storeName) {
        return
                Objects.requireNonNull(streamsBuilderFactoryBean
                                .getKafkaStreams())
                        .store(
                                StoreQueryParameters.fromNameAndType(
                                        storeName,
                                        QueryableStoreTypes.keyValueStore()
                                )
                        );
    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> ordersRevenueStore(String storeName) {

        return
                Objects.requireNonNull(streamsBuilderFactoryBean
                                .getKafkaStreams())
                        .store(
                                StoreQueryParameters.fromNameAndType(
                                        storeName,
                                        QueryableStoreTypes.keyValueStore()
                                )
                        );
    }

    public ReadOnlyWindowStore<String, Long> ordersWindowsCountStore(String storeName) {

        return
                Objects.requireNonNull(streamsBuilderFactoryBean
                                .getKafkaStreams())
                        .store(
                                StoreQueryParameters.fromNameAndType(
                                        storeName,
                                        QueryableStoreTypes.windowStore()
                                )
                        );
    }

    public ReadOnlyWindowStore<String, TotalRevenue> ordersWindowsRevenueStore(String storeName) {
        return
                Objects.requireNonNull(streamsBuilderFactoryBean
                                .getKafkaStreams())
                        .store(
                                StoreQueryParameters.fromNameAndType(
                                        storeName,
                                        QueryableStoreTypes.windowStore()
                                )
                        );
    }
}
