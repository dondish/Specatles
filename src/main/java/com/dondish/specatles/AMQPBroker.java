package com.dondish.specatles;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.rabbitmq.RabbitMQClient;
import io.vertx.rabbitmq.RabbitMQConsumer;
import io.vertx.rabbitmq.RabbitMQOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CompletableFuture;

/**
 * Broker implementation using AMQP with RabbitMQ
 */
public class AMQPBroker implements Broker {

    /**
     * The Vertx instance for async flow
     */
    private final Vertx vertx;

    /**
     * The RabbitMQ client
     */
    private final RabbitMQClient client;

    /**
     * The group / exchange
     */
    private final String group;

    /**
     * The subgroup (used to differentiate queues from one another)
     */
    private final String subgroup;

    /**
     * The logger
     */
    private final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Create a new AMQPBroker
     *
     * @param vertx    the vertx instance required for async operations
     * @param options  the RabbitMQ options such as host and user
     * @param group    the group (exchange)
     * @param subgroup the subgroup (used to differentiate queues from one another)
     */
    public AMQPBroker(@Nonnull Vertx vertx, @Nonnull RabbitMQOptions options, @Nonnull String group, String subgroup) {
        this.vertx = vertx;
        this.client = RabbitMQClient.create(vertx, options);
        this.group = group;
        this.subgroup = subgroup;
    }

    /**
     * Connect to the RabbitMQ server
     * @return a future that will return this instance if succeeded otherwise an exception
     */
    public CompletableFuture<AMQPBroker> connect() {
        CompletableFuture<AMQPBroker> cf = new CompletableFuture<>();
        client.start(ar -> {
            if (ar.failed()) {
                logger.info("Failed to connect to the AMQP Server!");
                cf.completeExceptionally(ar.cause());
            } else {
                client.exchangeDeclare(group, "direct", true, false, a -> {
                    if (a.failed()) {
                        cf.completeExceptionally(a.cause());
                    } else {
                        logger.info("Successfully Connected to the AMQP Server!");
                        cf.complete(this);
                    }
                });

            }
        });
        return cf;
    }

    /**
     * Disconnects from the RabbitMQ server
     * @return a future that will return this instance if succeeded otherwise an exception
     */
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
        final String address = String.join("", Arrays.asList(events));
        MessageConsumer<byte[]> consumer = eventBus().consumer(address);

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
                                    RabbitMQConsumer con = cres.result();
                                    con.handler(msg -> {
                                        eventBus().publish(address, Base64.getDecoder().decode(msg.body().getBytes()));
                                        client.basicAck(msg.envelope().deliveryTag(), false, a -> {
                                        });

                                    });
                                    if (c == events.length - 1) {
                                        cf.complete(consumer);
                                    }
                                    consumer.endHandler(a -> con.cancel());
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
        client.basicPublish(group, event, new JsonObject().put("body", data), res -> {
            if (res.succeeded()) {
                cf.complete(res.result());
            } else {
                cf.completeExceptionally(res.cause());
            }
        });

        return cf;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        disconnect()
                .thenRun(() -> completionHandler.handle(Future.succeededFuture()))
                .exceptionally(e -> {
                    completionHandler.handle(Future.failedFuture(e));
                    return null;
                });
    }
}
