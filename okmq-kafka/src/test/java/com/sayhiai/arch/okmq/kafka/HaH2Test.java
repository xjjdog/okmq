package com.sayhiai.arch.okmq.kafka;

import com.sayhiai.arch.okmq.api.Packet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class HaH2Test {

    @Test
    public void testH2SendAsync() {
        KafkaProducer producer = new ProducerBuilder()
                .defaultSerializer()
                .eanbleHa("h2")
                .any("okmq.h2.url", "jdbc:h2:file:D:/testh2/db;AUTO_SERVER=TRUE")
                .any("okmq.h2.user", "sa")
                .any("okmq.h2.passwd", "sa")
//                .any("okmq.h2.dataLength", 2097152)
                //.servers("10.30.94.8:9092")
                .servers("10.30.94.8:9092")
                .clientID("okMQProducerTest")
                .build();


        for (int i = 0; i < 999999; i++) {
            Packet packet = new Packet();
            packet.setTopic("okmq-test-topic");
            packet.setContent("i will send you a msg" + System.nanoTime());
            producer.sendAsync(packet, null);
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            producer.sendSerial(packet, 61000);
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
