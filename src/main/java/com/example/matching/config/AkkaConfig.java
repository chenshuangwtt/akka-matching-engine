package com.example.matching.config;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Props;

import com.example.matching.actor.EngineRootActor;
import com.typesafe.config.ConfigFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

@Configuration
public class AkkaConfig {

    @Bean(destroyMethod = "terminate")
    public ActorSystem<EngineRootActor.Command> actorSystem(
            @Value("${matching.persistence.enabled:true}") boolean persistenceEnabled,
            @Value("${matching.persistence.dir:data/events}") String persistenceDir,
            @Value("${matching.persistence.snapshot-threshold:1000}") int snapshotThreshold,
            @Value("${matching.persistence.compact-low-load-open-orders:64}")
                    int compactLowLoadOpenOrders,
            @Value("${matching.max-historical-orders-per-symbol:100000}") int maxHistoricalOrders,
            @Value("${matching.preload-symbols:}") String preloadSymbols) {
        return ActorSystem.create(
                EngineRootActor.create(
                        persistenceEnabled,
                        Path.of(persistenceDir),
                        snapshotThreshold,
                        compactLowLoadOpenOrders,
                        parsePreloadSymbols(preloadSymbols),
                        maxHistoricalOrders),
                "matching-engine-system",
                ConfigFactory.load(),
                Props.empty().withDispatcherFromConfig("akka.actor.matching-dispatcher"));
    }

    private List<String> parsePreloadSymbols(String preloadSymbols) {
        if (preloadSymbols == null || preloadSymbols.isBlank()) {
            return List.of();
        }
        return Arrays.stream(preloadSymbols.split(","))
                .map(String::trim)
                .filter(symbol -> !symbol.isEmpty())
                .toList();
    }
}
