package com.sayhiai.arch.okmq.api.producer.ha;

import com.sayhiai.arch.okmq.api.Packet;
import com.sayhiai.arch.okmq.api.producer.AbstractProducer;
import com.sayhiai.arch.okmq.api.producer.HaException;
import lombok.Getter;
import lombok.Setter;

import java.util.Properties;

/**
 * like
 * org.apache.kafka.clients.producer.ProducerInterceptor
 */
public interface HA {
    void close();

    void configure(Properties properties);

    void preSend(Packet packet) throws HaException;

    void postSend(Packet packet) throws HaException;

    void doRecovery(AbstractProducer producer) throws HaException;

    @Getter
    @Setter
    final class Config {
        private boolean ha = true;
        private String haType = "log";
        private long recoveryPeriod = 5000L;
    }
}


