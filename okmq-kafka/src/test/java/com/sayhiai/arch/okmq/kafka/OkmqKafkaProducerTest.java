package com.sayhiai.arch.okmq.kafka;

import com.sayhiai.arch.okmq.api.Packet;
import org.junit.jupiter.api.Test;

public class OkmqKafkaProducerTest {

    @Test
    public void testSendAsync() {
        OkmqKafkaProducer producer = new ProducerBuilder()
                .defaultSerializer()
                .eanbleHa("redis")
                .any("okmq.redis.mode", "single")
                .any("okmq.redis.endpoint", "127.0.0.1:6379")
                .any("okmq.redis.poolConfig.maxTotal", 100)
                //.servers("10.30.94.8:9092")
                .servers("localhost:9092")
                .clientID("okMQProducerTest")
                .build();


        for (int i = 0; i < 1; i++) {
            Packet packet = new Packet();
            packet.setTopic("okmq-test-topic");
            packet.setContent("i will send you a msg" + System.nanoTime());
            producer.sendAsync(packet, null);
//            producer.sendSerial(packet, 1000);
        }

        System.out.println("We'll be blue");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        producer.shutdown();
    }

    @Test
    public void testDBH2() {
        OkmqKafkaProducer producer = new ProducerBuilder()
                .defaultSerializer()
                .eanbleHa("h2")
                .any("okmq.h2.url", "jdbc:h2:file:D:/testh2/db;AUTO_SERVER=TRUE")
                .any("okmq.h2.user", "sa")
                .any("okmq.h2.passwd", "sa")
                //.servers("10.30.94.8:9092")
                .servers("localhost:9092")
                .clientID("okMQProducerTest")
                .build();


        for (int i = 0; i < 1; i++) {
            Packet packet = new Packet();
            packet.setTopic("okmq-test-topic");
            packet.setContent("i will send you a msg" + System.nanoTime());
            producer.sendAsync(packet, null);
//            producer.sendSerial(packet, 1000);
        }

        System.out.println("We'll be blue");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        producer.shutdown();
    }
}
