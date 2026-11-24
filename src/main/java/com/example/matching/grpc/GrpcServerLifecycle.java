package com.example.matching.grpc;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
public class GrpcServerLifecycle {

    private static final Logger log = LoggerFactory.getLogger(GrpcServerLifecycle.class);

    private final int port;
    private final List<BindableService> services;
    private Server server;

    public GrpcServerLifecycle(
            @Value("${grpc.server.port:50051}") int port, List<BindableService> services) {
        this.port = port;
        this.services = services;
    }

    @PostConstruct
    public void start() throws IOException {
        ServerBuilder<?> builder = ServerBuilder.forPort(port);
        for (BindableService service : services) {
            builder.addService(service);
        }
        server = builder.build().start();
        log.info("gRPC server started on port {}, services={}", port, services.size());
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            log.info("gRPC server stopping on port {}", port);
            server.shutdown();
        }
    }
}
