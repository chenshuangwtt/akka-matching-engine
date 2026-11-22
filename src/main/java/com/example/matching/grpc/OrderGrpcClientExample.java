package com.example.matching.grpc;

import com.example.order.grpc.CancelOrderRequest;
import com.example.order.grpc.CancelOrderResponse;
import com.example.order.grpc.GetBookRequest;
import com.example.order.grpc.GetBookResponse;
import com.example.order.grpc.OrderServiceGrpc;
import com.example.order.grpc.PlaceOrderRequest;
import com.example.order.grpc.PlaceOrderResponse;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class OrderGrpcClientExample {

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 50051;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();

        try {
            OrderServiceGrpc.OrderServiceBlockingStub stub = OrderServiceGrpc.newBlockingStub(channel);

            PlaceOrderResponse placeResponse = stub.placeOrder(
                    PlaceOrderRequest.newBuilder()
                            .setUserId(10001L)
                            .setClientOrderId("cli-10001")
                            .setUsername("demo-user")
                            .setSymbol("BTCUSDT")
                            .setSide(com.example.order.grpc.Side.SIDE_BUY)
                            .setPrice(10000L)
                            .setQuantity(2L)
                            .build()
            );

            System.out.println("placeOrder => success=" + placeResponse.getSuccess()
                    + ", message=" + placeResponse.getMessage()
                    + ", orderId=" + placeResponse.getOrderId());

            GetBookResponse bookResponse = stub.getBook(
                    GetBookRequest.newBuilder()
                            .setSymbol("BTCUSDT")
                            .build()
            );

            System.out.println("getBook => success=" + bookResponse.getSuccess()
                    + ", lines=" + bookResponse.getLinesCount());
            for (String line : bookResponse.getLinesList()) {
                System.out.println(line);
            }

            CancelOrderResponse cancelResponse = stub.cancelOrder(
                    CancelOrderRequest.newBuilder()
                            .setSymbol("BTCUSDT")
                            .setOrderId(placeResponse.getOrderId())
                            .build()
            );

            System.out.println("cancelOrder => success=" + cancelResponse.getSuccess()
                    + ", message=" + cancelResponse.getMessage());
        } finally {
            channel.shutdown();
            channel.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
