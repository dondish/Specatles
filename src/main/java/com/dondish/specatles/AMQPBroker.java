package com.dondish.specatles;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

public class AMQPBroker implements Broker {

    private final Vertx vertx;

    private final RabbitMQClient client;

    private final String group;

    private final String subgroup;

    private final Logger logger = LoggerFactory.getLogger(getClass());

    public AMQPBroker(@Nonnull Vertx vertx, @Nonnull RabbitMQOptions options, @Nonnull String group, String subgroup) {
        this.vertx = vertx;
        this.client = RabbitMQClient.create(vertx, options);
        this.group = group;
        this.subgroup = subgroup;
    }

    public CompletableFuture<AMQPBroker> connect() {
        CompletableFuture<AMQPBroker> cf = new CompletableFuture<>();
        client.start(ar -> {
            if (ar.failed()) {
                cf.completeExceptionally(ar.cause());
            } else {
                logger.info("Successfully Connected to the AMQP Server!");
                cf.complete(this);
            }
        });
        return cf;
    }

    public CompletableFuture<AMQPBroker> disconnect(){
        CompletableFuture<AMQPBroker> cf = new CompletableFuture<>();
        client.stop(ar -> {
            if (ar.failed()){
                cf.completeExceptionally(ar.cause());
            } else {
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
    public CompletableFuture<MessageConsumer<byte[]>> on(@Nonnull String... events) {
        CompletableFuture<MessageConsumer<byte[]>> cf = new CompletableFuture<>();
        MessageConsumer<byte[]> consumer = eventBus().consumer(String.join("", Arrays.asList(events)));

        for (int i = 0; i < events.length; i++) {
            String event = events[i];
            String queue = String.format("%s%s%s", group, subgroup == null ? "" : subgroup, event);
            final int c = i;
            client.queueDeclare(queue,true, false, false, res -> {
                if (res.failed()) {
                    cf.completeExceptionally(res.cause());
                } else {
                    client.queueBind(queue, group, event, r -> {
                        if (res.failed())
                            cf.completeExceptionally(res.cause());
                        else {
                            client.basicConsumer(queue, cres -> {
                                if (cres.failed())
                                    cf.completeExceptionally(cres.cause());
                                else {
                                    cres.result().handler(msg -> {
                                        eventBus().publish(String.join("", Arrays.asList(events)), msg.body().getBytes());
                                        client.basicAck(msg.envelope().deliveryTag(), false, ares -> {
                                            if (c == events.length - 1){
                                                cf.complete(consumer);
                                            }
                                        });
                                    }
                                    );

                                }
                            });
                        }
                    });
                }
            });

        }

        return cf;
    }

    @Override
    public CompletableFuture<Void> publish(@Nonnull String event, @Nonnull byte[] data) {
        CompletableFuture<Void> cf = new CompletableFuture<>();

        client.basicPublish(group, event, new JsonObject(Buffer.buffer(data)), res -> {
            if (res.failed()) {
                cf.completeExceptionally(res.cause());
            } else {
                cf.complete(res.result());
            }
        });

        return cf;
    }
}
