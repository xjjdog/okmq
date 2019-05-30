package com.sayhiai.arch.okmq.api.producer;


import com.sayhiai.arch.okmq.api.Packet;
import com.sayhiai.arch.okmq.api.producer.ha.HA;
import com.sayhiai.arch.okmq.api.producer.ha.Ha2SimpleLog;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractProducer {
    private AbstractProducer() {
    }

    public AbstractProducer(boolean ha, String haMode) {
        this.config = new HA.Config();
        config.setHa(ha);
        config.setHaType(haMode);
    }


    protected HA ha;
    protected HA.Config config;
    protected Properties properties;

    private ScheduledExecutorService recoveryExecutor =
            new ScheduledThreadPoolExecutor(1);


    private static HashMap<String, String> registers = new HashMap<>();

    static {
        register("log", "com.sayhiai.arch.okmq.api.producer.ha.Ha2SimpleLog");
        register("redis", "com.sayhiai.arch.okmq.api.producer.ha.Ha2Redis");
    }

    public static final void register(String name, String haClass) {
        registers.putIfAbsent(name, haClass);
    }


    public void init(final Properties properties) {
        this.properties = properties;

        if (config.isHa()) {

            //recovery period
            Object recoveryPeriodVal = properties.get("okmq.ha.recoveryPeriod");
            if (null != recoveryPeriodVal) {
                long value = Long.valueOf(recoveryPeriodVal + "");
                if (value > 5000L) {//magic
                    config.setRecoveryPeriod(value);
                }
            }

            final String haClass = registers.get(config.getHaType());
            try {
                ha = (HA) Class.forName(haClass).getDeclaredConstructors()[0].newInstance();
            } catch (Exception ex) {
                log.error("okmq:ha invoke error", ex);
            }
            if (null == ha) {
                log.warn("NO HA instance supplied! use the Ha2SimpleLog default!");
                ha = new Ha2SimpleLog();
            }
            ha.configure(properties);
        }
        doInit();

        //Begin do recovery!
        doRecovery();

    }

    protected abstract void doInit();

    public abstract void doShutdown();

    public void shutdown() {
        this.doShutdown();
        if (config.isHa()) {
            ha.close();
        }
        recoveryExecutor.shutdown();
    }

    public abstract SendResult sendSerial(Packet packet, long timeoutMillis);

    public SendResult sendSerial(Packet packet) {
        return sendSerial(packet, -1);
    }

    public abstract SendResult sendAsync(Packet packet);

    public abstract SendResult sendAsync(Packet packet, Callback callback);

    public abstract <T> T getBackend();


    private long preRecovery = System.currentTimeMillis();

    private void doRecovery() {
        if (config.isHa()) {
            final long rVal = config.getRecoveryPeriod();
            recoveryExecutor.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    long now = System.currentTimeMillis();
                    if (now - preRecovery > rVal) {
                        try {
                            ha.doRecovery(AbstractProducer.this);
                        } catch (HaException e) {
                            log.error("okmq:recovery", e);
                        }
                        preRecovery = System.currentTimeMillis();
                    } else {
                        log.info("recovery request ignored! {}ms passed.", now - preRecovery);
                    }
                }
            }, rVal, rVal, TimeUnit.MILLISECONDS);
        }
    }

}
