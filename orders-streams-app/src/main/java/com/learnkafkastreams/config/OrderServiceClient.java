package com.learnkafkastreams.config;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.OrderCountPerStoreDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.List;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderServiceClient {
    private final WebClient webClient;

    public List<OrderCountPerStoreDTO> retrieveOrdersCountsByHostPath(HostInfoDTO hostInfoDTO, String orderType) {
        var basePath = "http://"+ hostInfoDTO.host() + ":" + hostInfoDTO.port();

        final String url = UriComponentsBuilder
                .fromHttpUrl(basePath)
                .path("/v1/orders/count/{order_type}")
                .buildAndExpand(orderType)
                .toString();
        return webClient
                .get()
                .uri(url)
                .retrieve()
                .bodyToFlux(OrderCountPerStoreDTO.class)
                .collectList()
                .block();
    }
}
