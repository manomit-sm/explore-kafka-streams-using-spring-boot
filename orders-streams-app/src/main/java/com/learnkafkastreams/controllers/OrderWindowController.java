package com.learnkafkastreams.controllers;

import com.learnkafkastreams.service.OrderWindowService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/v1/orders")
@RequiredArgsConstructor
public class OrderWindowController {

    private final OrderWindowService orderWindowService;

    @GetMapping("/windows/count/{order_type}")
    public ResponseEntity<?> orderWindowCount(@PathVariable("order_type") String orderType) {
        return ResponseEntity.ok(orderWindowService.getOrderByWindowCount(orderType));
    }

    @GetMapping("/windows/count")
    public ResponseEntity<?> orderWindowCountAll(
            @RequestParam(value = "from_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromTime,
            @RequestParam(value = "to_time", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toTime
    ) {
        if (fromTime != null && toTime != null)
            return ResponseEntity.ok(orderWindowService.getOrderByWindowCountAll(fromTime, toTime));
        return ResponseEntity.ok(orderWindowService.getOrderByWindowCountAll());
    }

    @GetMapping("/windows/revenue/{order_type}")
    public ResponseEntity<?> orderRevenue(@PathVariable("order_type") String orderType) {
        return ResponseEntity.ok(orderWindowService.getOrderRevenueByWindow(orderType));
    }
}
