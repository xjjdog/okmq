package com.sayhiai.arch.okmq.api.producer.ha;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sayhiai.arch.okmq.api.Packet;
import com.sayhiai.arch.okmq.api.producer.AbstractProducer;
import com.sayhiai.arch.okmq.api.producer.HaException;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.*;
import redis.clients.jedis.params.SetParams;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
public class Ha2Redis implements HA {
    JedisPoolAbstract pool;
    JedisCluster cluster;
    boolean isCluster;

    final static String RedisKeyPrefix = "okmq:";

    final static String RedisRecoveryLockKey = RedisKeyPrefix + "recovery:lock";
    final static String LockIdentify = UUID.randomUUID().toString();

    final static String IdxHashKey = RedisKeyPrefix + "indexhash";

    private final static String Props_Prefix = "okmq.redis.";

    private final ObjectMapper mapper = new ObjectMapper();

    private String getInnerIdxHashKeyByDate(long millis) {
        //5min every key
        final int time = (int) (millis / config.getSplitMillis());
        return RedisKeyPrefix + time;
    }


    @Override
    public void close() {
        if (isCluster) {
            this.cluster.close();
        } else {
            pool.close();
        }
    }

    final Ha2RedisConfig config = new Ha2RedisConfig();

    @Override
    public void configure(Properties properties) {
        final JedisPoolConfig poolConfig = new JedisPoolConfig();
        config.setPoolConfig(poolConfig);

        try {
            BeanInfo beanInfo = Introspector.getBeanInfo(Ha2RedisConfig.class);
            for (PropertyDescriptor desc : beanInfo.getPropertyDescriptors()) {
                if (desc.getPropertyType().equals(JedisPoolConfig.class)) {

                    BeanInfo beanInfoLvl2 = Introspector.getBeanInfo(JedisPoolConfig.class);
                    for (PropertyDescriptor descLvl2 : beanInfoLvl2.getPropertyDescriptors()) {
                        final String name = descLvl2.getName();
                        final Object value = properties.get(Props_Prefix + "poolConfig." + name);
                        if (null != value && !"".equals(value)) {
                            Method method = descLvl2.getWriteMethod();
                            method.invoke(poolConfig, value);
                        }
                    }
                } else {
                    final String name = desc.getName();
                    final Object value = properties.get(Props_Prefix + name);
                    if (null != value && !"".equals(value)) {
                        Method method = desc.getWriteMethod();
                        method.invoke(config, properties.get(Props_Prefix + name));
                    }
                }
            }
        } catch (Exception e) {
            log.error("error while config redis . pls check it !!");
        }


        isCluster = false;

        switch (config.getMode().toLowerCase()) {
            case Ha2RedisConfig.REDIS_MODE_SINGLE: {
                final String[] parts = config.getEndpoint().split(":");
                if (parts.length != 2) {
                    throw new RuntimeException("okmq:redis:config error: ex: 127.0.0.1:6379");
                }

                final String host = parts[0];
                final int port = Integer.valueOf(parts[1]);

                pool = new JedisPool(config.getPoolConfig(), host, port, config.getConnectionTimeout());
            }
            break;
            case Ha2RedisConfig.REDIS_MODE_SENTINEL: {
                final String[] parts = config.getEndpoint().split(",");

                Set<String> hostAndPorts = Arrays.stream(parts)
                        .map(item -> item.split(":"))
                        .map(item -> new HostAndPort(item[0], Integer.valueOf(item[1])).toString())
                        .collect(Collectors.toSet());

                pool = new JedisSentinelPool(config.getMasterName(), hostAndPorts, config.getPoolConfig());
            }
            break;
            case Ha2RedisConfig.REDIS_MODE_CLUSTER: {
                final String[] parts = config.getEndpoint().split(",");
                Set<HostAndPort> hostAndPorts = Arrays.stream(parts)
                        .map(item -> item.split(":"))
                        .map(item -> new HostAndPort(item[0], Integer.valueOf(item[1])))
                        .collect(Collectors.toSet());

                cluster = new JedisCluster(hostAndPorts, config.getPoolConfig());
                isCluster = true;
            }
            break;
            default:
                throw new RuntimeException("okmq:redis:no redis mode supply. ex:  single|sentinel|cluster");
        }
    }

    @Override
    public void preSend(Packet packet) throws HaException {

        final String idxKey = getInnerIdxHashKeyByDate(packet.getTimestamp());
        final String key = String.format("%s:%s", RedisKeyPrefix, packet.getIdentify());


        String value = "";
        try {
            value = mapper.writeValueAsString(packet);
        } catch (JsonProcessingException e) {
            throw new HaException("okmq:redis:json error:" + packet);
        }

        if (isCluster) {
            if (!cluster.hexists(IdxHashKey, idxKey)) {
                cluster.hset(IdxHashKey, idxKey, "");
            }
            cluster.hset(idxKey, key, value);
        } else {
            try (Jedis jedis = pool.getResource()) {
                if (!jedis.hexists(IdxHashKey, idxKey)) {
                    jedis.hset(IdxHashKey, idxKey, "");
                }
                jedis.hset(idxKey, key, value);
            } catch (Exception ex) {
                throw new HaException("okmq:redis:" + ex.getMessage());
            }
        }
    }

    @Override
    public void postSend(Packet packet) throws HaException {
        final String key = String.format("%s:%s", RedisKeyPrefix, packet.getIdentify());
        final String idxKey = getInnerIdxHashKeyByDate(packet.getTimestamp());

        if (isCluster) {
            cluster.hdel(idxKey, key);
        } else {
            try (Jedis jedis = pool.getResource()) {
                jedis.hdel(idxKey, key);
            } catch (Exception ex) {
                throw new HaException("okmq:redis:" + ex.getMessage());
            }
        }
    }

    @Override
    public void doRecovery(AbstractProducer producer) throws HaException {
        if (isCluster) {
            this.doRecoveryClusterMode(producer);
        } else {
            this.doRecoveryJedisMode(producer);
        }
    }

    private boolean lock() {
        SetParams setParams = new SetParams().px(config.getLockPx()); //expire  px millis

        String result = "";
        if (isCluster) {
            result = cluster.set(RedisRecoveryLockKey, LockIdentify, setParams);
        } else {
            try (Jedis jedis = pool.getResource()) {
                result = jedis.set(RedisRecoveryLockKey, LockIdentify, setParams);
            } catch (Exception ex) {
                log.error("okmq:redis:recovery: " + ex.getMessage());
                return false;
            }
        }
        if ("OK".equals(result)) {
            return true;
        } else {
            log.info("okmq:redis:recovery:can not get the lock! ");
        }
        return false;
    }

    private boolean unlock() {
        if (isCluster) {
            String id = cluster.get(RedisRecoveryLockKey);
            if (null == id || "".equals(id)) {
                return true;
            } else {
                if (id.equals(LockIdentify)) {
                    cluster.del(RedisRecoveryLockKey);
                    return true;
                }
            }
        } else {
            try (Jedis jedis = pool.getResource()) {
                String id = jedis.get(RedisRecoveryLockKey);
                if (null == id || "".equals(id)) {
                    return true;
                } else {
                    if (id.equals(LockIdentify)) {
                        jedis.del(RedisRecoveryLockKey);
                        return true;
                    }
                }
            } catch (Exception ex) {
                log.error("okmq:redis:recovery: " + ex.getMessage());
                return true;
            }

        }
        return false;
    }

    private void doRecoveryClusterMode(AbstractProducer producer) throws HaException {
        final long begin = System.currentTimeMillis();

        if (!lock()) return;

        List<String> keys = new ArrayList(cluster.hgetAll(IdxHashKey).keySet());
        Collections.sort(keys);

        for (String key : keys) {
            String cur = ScanParams.SCAN_POINTER_START;
            for (; ; ) {
                ScanResult<Map.Entry<String, String>> result = cluster.hscan(key, cur);

                for (Map.Entry<String, String> kv : result.getResult()) {
                    if (System.currentTimeMillis() - begin >= config.getLockPx()) {
                        //bala bala
                        if (unlock()) return;
                    }

                    final String k = kv.getKey();
                    final String v = kv.getValue();

                    final Packet p = mapper.convertValue(v, Packet.class);
                    if (System.currentTimeMillis() - p.getTimestamp() > 10 * 1000L) {
                        producer.sendAsync(p);
                        cluster.hdel(key, k);
                    }
                }

                cur = result.getCursor();
                if ("0".equals(cur)) {
                    cluster.hdel(IdxHashKey, key);
                    break;
                }
            }
        }

        if (System.currentTimeMillis() - begin < config.getLockPx()) {
            //bala bala
            unlock();
        }
    }

    private void doRecoveryJedisMode(AbstractProducer producer) throws HaException {
        try (Jedis jedis = pool.getResource()) {
            final long begin = System.currentTimeMillis();

            if (!lock()) return;

            List<String> keys = new ArrayList(jedis.hgetAll(IdxHashKey).keySet());
            Collections.sort(keys);

            for (String key : keys) {
                String cur = ScanParams.SCAN_POINTER_START;
                for (; ; ) {
                    ScanResult<Map.Entry<String, String>> result = jedis.hscan(key, cur);

                    for (Map.Entry<String, String> kv : result.getResult()) {
                        if (System.currentTimeMillis() - begin >= config.getLockPx()) {
                            //bala bala
                            if (unlock()) return;
                        }

                        final String k = kv.getKey();
                        final String v = kv.getValue();

                        final Packet p = mapper.convertValue(v, Packet.class);
                        if (System.currentTimeMillis() - p.getTimestamp() > 10 * 1000L) {
                            producer.sendAsync(p);
                            jedis.hdel(key, k);
                        }
                    }

                    cur = result.getCursor();
                    if ("0".equals(cur)) {
                        jedis.hdel(IdxHashKey, key);
                        break;
                    }
                }
            }

            if (System.currentTimeMillis() - begin < config.getLockPx()) {
                //bala bala
                unlock();
            }
        } catch (Exception ex) {
            throw new HaException("okmq:redis:" + ex.getMessage());
        }
    }


}

@Setter
@Getter
class Ha2RedisConfig {
    public static final String REDIS_MODE_SINGLE = "single";
    public static final String REDIS_MODE_SENTINEL = "sentinel";
    public static final String REDIS_MODE_CLUSTER = "cluster";

    private String mode = "";
    private String endpoint = "";
    private int connectionTimeout = 2000;
    private int soTimeout = 2000;
    private long lockPx = 1000L * 60 * 5;
    private long splitMillis = 1000L * 60 * 5;


    private JedisPoolConfig poolConfig;


    /**
     * Only used by sentinel mode
     */
    private String masterName = "";

}
