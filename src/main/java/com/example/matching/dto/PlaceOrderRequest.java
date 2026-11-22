package com.example.matching.dto;

import com.example.matching.domain.Side;
import lombok.Data;


@Data
public class PlaceOrderRequest {

    private String userId;
    private String clientOrderId;
    private String symbol;
    private Side side;
    private long price;
    private long quantity;
}
