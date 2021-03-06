/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.dondish.specatles;

import com.mewna.catnip.Catnip;
import com.mewna.catnip.shard.DiscordEvent;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.rabbitmq.RabbitMQOptions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@ExtendWith(VertxExtension.class)
public class LibraryTest {
    private final static Logger log = LoggerFactory.getLogger(LibraryTest.class);

    @Test
    public void test_job() {
        Catnip catnip = Catnip.catnip(System.getenv("TOKEN"));
        VertxTestContext context = new VertxTestContext();
        try {
            new AMQPBroker(Vertx.vertx(), new RabbitMQOptions().setHost("localhost").setUser("guest").setPassword("guest"), "gateway", null).connect().thenApply(broker -> {
                log.info("Broker Connected");

                broker.on("ready").thenApply(messageConsumer -> {
                    log.info("Broker added ready event handler");
                    messageConsumer.handler(message -> {
                        context.verify(() -> {
                            JsonObject msg = new JsonObject(Buffer.buffer(message.body()));
                            Assert.assertEquals(msg.getJsonObject("d").getJsonObject("user").getLong("idAsLong").longValue(), 553276192300466176L);
                            context.completeNow();
                        });

                    });
                    catnip.on(DiscordEvent.READY, e -> broker.publish("ready", e.toJson().encodePrettily().getBytes(StandardCharsets.UTF_8))
                            .thenRun(() -> log.info("Message was published."))
                            .exceptionally(er -> {
                                context.failNow(er);
                                return null;
                            })
                    );

                    catnip.connect();
                    return messageConsumer;
                }).exceptionally(e -> {
                    context.failNow(e);
                    return null;
                });


                return null;
            }).exceptionally(e -> {
                context.failNow(e);
                return null;
            });
            Assert.assertTrue(context.awaitCompletion(10, TimeUnit.SECONDS));
        } catch (Exception e) {
            context.failNow(e);
        }
    }
}
