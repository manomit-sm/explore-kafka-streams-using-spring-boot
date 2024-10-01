package com.learnkafkastreams.controllers;

import com.learnkafkastreams.domain.AllOrdersCountPerStore;
import com.learnkafkastreams.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.net.UnknownHostException;
import java.util.List;

@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(
            @PathVariable("order_type") String orderType,
            @RequestParam(value = "location_id", required = false) String locationId
    ) throws UnknownHostException {
        if (StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getOrdersCountByLocation(orderType, locationId));
        }
        return ResponseEntity.ok(orderService.getOrdersCount(orderType));
    }

    @GetMapping("/count")
    public List<AllOrdersCountPerStore> totalOrdersCount(
    ) {
        return orderService.totalOrdersCount();
    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> revenueByOrderType(
            @PathVariable("order_type") String orderType
            //@RequestParam(value = "location_id", required = false) String locationId
    ) {
        /*if (StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getOrdersCountByLocation(orderType, locationId));
        }*/
        return ResponseEntity.ok(orderService.revenueByOrderTYpe(orderType));
    }
}
