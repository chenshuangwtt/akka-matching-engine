package com.example.matching.engine;

import com.example.matching.domain.Order;
import com.example.matching.domain.Trade;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class MatchResult {
    private final List<Trade> trades = new ArrayList<>();
    private boolean booked;
    private Order finalOrder;
    private String message;
}
