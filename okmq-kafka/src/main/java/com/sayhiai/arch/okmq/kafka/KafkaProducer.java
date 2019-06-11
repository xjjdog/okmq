package com.sayhiai.arch.okmq.kafka;

import com.sayhiai.arch.okmq.api.Packet;
import com.sayhiai.arch.okmq.api.producer.AbstractProducer;
import com.sayhiai.arch.okmq.api.producer.Callback;
import com.sayhiai.arch.okmq.api.producer.HaException;
import com.sayhiai.arch.okmq.api.producer.SendResult;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class KafkaProducer extends AbstractProducer {

    /**
     * @see KafkaProducer#getBackend()
     */
    @Getter
    org.apache.kafka.clients.producer.KafkaProducer producer;

    public KafkaProducer(boolean ha, String haMode) {
        super(ha, haMode);
    }

    public void doInit() {
        producer = new org.apache.kafka.clients.producer.KafkaProducer(properties);
    }

    @Override
    public void doShutdown() {
        producer.flush();
        producer.close();
    }

    @Override
    /**
     * ha.pre or post method , is more like the interceptor
     * @see  org.apache.kafka.clients.producer.ProducerInterceptor
     */
    public SendResult sendSerial(Packet packet, long timeoutMillis) {
        ///////PRE/////////
        if (config.isHa()) {
            try {
                ha.preSend(packet);
            } catch (HaException ex) {
                log.error(ex.getMessage());
            }
        }

        SendResult result = new SendResult();
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(packet.getTopic(), packet.getContent());
        Future<RecordMetadata> future = producer.send(record);

        try {
            RecordMetadata metadata = future.get(timeoutMillis, TimeUnit.MILLISECONDS);

            log.info("okmq:kafka:{}|{}|cost:{}", metadata.offset(), metadata.partition(), System.currentTimeMillis() - packet.getTimestamp());

            result.setCode(SendResult.OK);
        } catch (Exception e) {
            log.error("okmq:sendSerial:" + e.getMessage() + packet);

            result.setCode(SendResult.ERROR);
            result.setMsg(e.getMessage());
        }

        ///////END/////////
        if (config.isHa()) {
            try {
                ha.postSend(packet);
            } catch (HaException ex) {
                log.error(ex.getMessage());
            }
        }
        return result;
    }

    @Override
    public SendResult sendAsync(Packet packet) {
        return
                this.sendAsync(packet, null);
    }

    @Override
    public SendResult sendAsync(final Packet packet, final Callback callback) {
        if (config.isHa()) {
            try {
                ha.preSend(packet);
            } catch (HaException ex) {
                log.error(ex.getMessage());
            }
        }


        SendResult result = new SendResult();
        result.setCode(SendResult.OK);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(packet.getTopic(), packet.getContent());
        producer.send(record, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(final RecordMetadata metadata, final Exception exception) {
                if (null == exception) {
                    log.info("okmq:kafka:{}|{}|cost:{}", metadata.offset(), metadata.partition(), System.currentTimeMillis() - packet.getTimestamp());
                    if (config.isHa()) {
                        try {
                            ha.postSend(packet);
                        } catch (HaException ex) {
                            log.error(ex.getMessage());
                        }
                    }
                } else {
                    log.info("okmq:kafka:" + exception.getMessage(), exception);
                }
                if (null != callback) {
                    callback.callback(packet, metadata, exception);
                }
            }
        });
        return result;
    }

    @Override
    public <T> T getBackend() {
        return (T) this.producer;
    }

}
