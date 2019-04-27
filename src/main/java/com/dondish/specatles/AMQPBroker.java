package com.dondish.specatles;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.ext.amqp.AmqpClient;
import io.vertx.ext.amqp.AmqpClientOptions;
import io.vertx.ext.amqp.AmqpConnection;
import io.vertx.reactivex.core.eventbus.MessageConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

public class AMQPBroker implements Broker {

    private final Vertx vertx;

    private final AmqpClient client;

    private AmqpConnection connection;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public AMQPBroker(@Nonnull Vertx vertx, @Nonnull AmqpClientOptions options) {
        this.vertx = vertx;
        this.client = AmqpClient.create(vertx, options);
    }

    public CompletableFuture<AMQPBroker> connect() {
        CompletableFuture<AMQPBroker> cf = new CompletableFuture<>();
        client.connect(ar -> {
            if (ar.failed()) {
                cf.completeExceptionally(ar.cause());
            } else {
                logger.info("Successfully Connected to the AMQP Server!");
                connection = ar.result();
                cf.complete(this);
            }
        });
        return cf;
    }

    @Override
    public Vertx vertx() {
        return vertx;
    }

    @Override
    public EventBus eventBus() {
        return vertx.eventBus();
    }

    @Override
    public MessageConsumer<byte[]> on(@Nonnull String... events) {
        return null;
    }

    @Override
    public CompletableFuture<byte[]> publish(@Nonnull String event, @Nonnull byte[] data) {
        return null;
    }
}
