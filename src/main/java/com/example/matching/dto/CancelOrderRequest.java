package com.example.matching.dto;

import lombok.Data;

@Data
public class CancelOrderRequest {
    private String symbol;
    private String orderId;
}
