package com.example.matching.controller;

import com.example.matching.dto.ApiResponse;
import com.example.matching.dto.CancelOrderRequest;
import com.example.matching.dto.PlaceOrderRequest;
import com.example.matching.engine.MatchResult;
import com.example.matching.service.OrderApplicationService;
import com.example.matching.service.OrderConflictException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.List;
import java.util.concurrent.CompletionStage;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderApplicationService orderApplicationService;

    public OrderController(OrderApplicationService orderApplicationService) {
        this.orderApplicationService = orderApplicationService;
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ApiResponse<Void>> unavailable(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(ApiResponse.fail(ex.getMessage()));
    }

    @ExceptionHandler(OrderConflictException.class)
    public ResponseEntity<ApiResponse<Void>> conflict(OrderConflictException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(ApiResponse.fail(ex.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiResponse<Void>> invalidOrder(IllegalArgumentException ex) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(ApiResponse.fail(ex.getMessage()));
    }

    @PostMapping("/place")
    public CompletionStage<ApiResponse<MatchResult>> place(@RequestBody PlaceOrderRequest req) {
        return orderApplicationService.placeOrder(
                "",
                req.getClientOrderId(),
                req.getUserId(),
                req.getSymbol(),
                req.getSide(),
                req.getPrice(),
                req.getQuantity()
        ).thenApply(ApiResponse::ok);
    }

    @PostMapping("/cancel")
    public CompletionStage<ApiResponse<Boolean>> cancel(@RequestBody CancelOrderRequest req) {
        return orderApplicationService.cancelOrder(req.getSymbol(), req.getOrderId())
                .thenApply(ApiResponse::ok);
    }

    @GetMapping("/book/{symbol}")
    public CompletionStage<ApiResponse<List<String>>> book(@PathVariable String symbol) {
        return orderApplicationService.getBook(symbol).thenApply(ApiResponse::ok);
    }

    @GetMapping("/book")
    public CompletionStage<ApiResponse<List<String>>> bookByQuery(@RequestParam String symbol) {
        return orderApplicationService.getBook(symbol).thenApply(ApiResponse::ok);
    }
}
