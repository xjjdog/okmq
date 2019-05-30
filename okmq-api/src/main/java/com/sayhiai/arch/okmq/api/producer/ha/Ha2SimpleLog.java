package com.sayhiai.arch.okmq.api.producer.ha;

import com.sayhiai.arch.okmq.api.Packet;
import com.sayhiai.arch.okmq.api.producer.AbstractProducer;
import com.sayhiai.arch.okmq.api.producer.HaException;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

@Slf4j
public class Ha2SimpleLog implements HA {
    @Override
    public void close() {
        //pass
    }

    @Override
    public void configure(Properties properties) {
    }

    @Override
    public void preSend(Packet packet) throws HaException {
        log.info("ping:okmq:{}|{}|{}|{}", packet.getTimestamp(), packet.getIdentify(), packet.getTopic(), packet.getContent());
    }

    @Override
    public void postSend(Packet packet) throws HaException {
        log.info("pong:okmq:{}|{}", packet.getIdentify(), packet.getTopic());
    }

    @Override
    public void doRecovery(AbstractProducer producer) throws HaException {
        log.info("HA mode not support recovery mode yet!");
    }
}
