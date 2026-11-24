package com.example.matching.persistence;

import com.example.matching.domain.Order;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
public class OrderBookSnapshot {

    private long processedCount;
    private Long lastAppliedSequence;
    private int formatVersion;
    private List<Order> openOrders = new ArrayList<>();
    private List<Order> allOrders;
}
