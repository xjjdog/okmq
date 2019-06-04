package com.sayhiai.arch.okmq.kafka;

import com.sayhiai.arch.okmq.api.producer.ha.HA;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerBuilder {
    Properties props = new Properties();
    HA.Config haConfig = new HA.Config();

    public ProducerBuilder() {
        this.any(ProducerConfig.ACKS_CONFIG, "1");
        this.any(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        this.any(ProducerConfig.RETRIES_CONFIG, "1");
    }

    public ProducerBuilder defaultSerializer() {
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return this;
    }


    public ProducerBuilder any(final Object key, final Object value) {
        props.put(key, value);
        return this;
    }

    public ProducerBuilder maxBlockMs(long ms) {
        this.any(ProducerConfig.MAX_BLOCK_MS_CONFIG, ms);
        return this;
    }

    public ProducerBuilder eanbleHa(String mode) {
        haConfig.setHa(true);
        haConfig.setHaType(mode);
        return this;
    }

    public ProducerBuilder servers(final String servers) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        return this;
    }

    public ProducerBuilder clientID(final String name) {
        props.put(ProducerConfig.CLIENT_ID_CONFIG, name);
        return this;
    }


    public KafkaProducer build() {
        KafkaProducer producer = new KafkaProducer(this.haConfig.isHa(), this.haConfig.getHaType());
        producer.init(this.props);
        return producer;
    }

}
