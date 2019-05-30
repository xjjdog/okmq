>本工具的核心思想就是：赌。只有两个基础组件同时死亡，才会受到`严重影响`。哦，断电除外。

mq是个好东西，我们都在用。这也决定了mq应该是高高高可用的。某团就因为这个组件，出了好几次生产事故，呵呵。

大部分业务系统，要求的消息语义都是`at least once`，即都会有重复消息，但保证不会丢。即使这样，依然有很多问题：

**一、mq可用性无法保证。** mq的意外死亡，造成生产端发送失败。很多消息要通过扒取日志进行回放，成本高耗时长。

**二、mq阻塞业务正常进行。**  mq卡顿或者网络问题，会造成业务线程卡在mq的发送方法上，正常业务进行不下去，造成灾难性的后果。

**三、消息延迟。** mq死了就用不着说了，消息还没投胎就已死亡。消息延迟主要是客户端消费能力不强，或者是消费通道单一造成的。

**使用组合存储来保证消息的可靠投递，就是`okmq`。**

>注意：okmq注重的是可靠性。对于顺序性、事务等其他要素，不予考虑。当然，速度是必须的。


# 设计想法

我即使用两套redis来模拟一些mq操作，都会比现有的一些解决方案要强。但这肯定不是我们需要的，因为redis的堆积能力太有限，内存占用率直线上升的感觉并不太好。

但我们可以用redis来作为额外的发送确认机制。这个想法，在[《使用多线程增加kafka消费能力》](https://mp.weixin.qq.com/s/FhLuPSwJFCdQ1oCWXf-V2Q)一文中曾经提到过，现在到了实现的时候了。
## 首先看下使用Api

```
OkmqKafkaProducer producer = new ProducerBuilder()
.defaultSerializer()
.eanbleHa("redis")
.any("okmq.redis.mode", "single")
.any("okmq.redis.endpoint", "127.0.0.1:6379")
.any("okmq.redis.poolConfig.maxTotal", 100)
.servers("localhost:9092")
.clientID("okMQProducerTest")
.build();

Packet packet = new Packet();
packet.setTopic("okmq-test-topic");
packet.setContent("i will send you a msg");
producer.sendAsync(packet, null);
producer.shutdown();
```
## 以redis为例
![](https://user-gold-cdn.xitu.io/2019/5/30/16b0821088eabaec?w=702&h=398&f=png&s=134741)
我们按照数字标号来介绍：

**1、**在消息发送到kafka之前，首先入库redis。由于后续回调需要用到一个唯一表示，我们在packet包里添加了一个uuid。

**2、**调用底层的api，进行真正的消息投递。

**3、**通过监听kafka的回调，删除redis中对应的key。在这里可以得到某条消息确切的的ack时间。那么长时间没有删除的，就算是投递失败的消息。

**4、**后台会有一个线程进行这些失败消息的遍历和重新投递。我们叫做recovery。最复杂的也就是这一部分。对于redis来说，会首先争抢一个持续5min的锁，然后遍历相关hashkey。

所以，对于以上代码，redis发出以下命令：
```
1559206423.395597 [0 127.0.0.1:62858] "HEXISTS" "okmq:indexhash" "okmq:5197354"
1559206423.396670 [0 127.0.0.1:62858] "HSET" "okmq:indexhash" "okmq:5197354" ""
1559206423.397300 [0 127.0.0.1:62858] "HSET" "okmq:5197354" "okmq::2b9b33fd-95fd-4cd6-8815-4c572f13f76e" "{\"content\":\"i will send you a msg104736623015238\",\"topic\":\"okmq-test-topic\",\"identify\":\"2b9b33fd-95fd-4cd6-8815-4c572f13f76e\",\"timestamp\":1559206423318}"
1559206423.676212 [0 127.0.0.1:62858] "HDEL" "okmq:5197354" "okmq::2b9b33fd-95fd-4cd6-8815-4c572f13f76e"
1559206428.327788 [0 127.0.0.1:62861] "SET" "okmq:recovery:lock" "01fb85a9-0670-40c3-8386-b2b7178d4faf" "px" "300000"
1559206428.337930 [0 127.0.0.1:62858] "HGETALL" "okmq:indexhash"
1559206428.341365 [0 127.0.0.1:62858] "HSCAN" "okmq:5197354" "0"
1559206428.342446 [0 127.0.0.1:62858] "HDEL" "okmq:indexhash" "okmq:5197354"
1559206428.342788 [0 127.0.0.1:62861] "GET" "okmq:recovery:lock"
1559206428.343119 [0 127.0.0.1:62861] "DEL" "okmq:recovery:lock"
```

## 以上问题解答

#### 所以对于以上的三个问题，回答如下：

**一、mq可用性无法保证。**

为什么要要通过事后进行恢复呢？我把recovery机制带着不是更好么？通过对未收到ack的消息进行遍历，可以把这个过程做成自动化。


**二、mq阻塞业务正常进行。**  

通过设置kafka的MAX_BLOCK_MS_CONFIG
参数，其实是可以不阻塞业务的，但会丢失消息。我可以使用其他存储来保证这些丢失的消息重新发送。

**三、消息延迟。** 

mq死掉了，依然有其他备用通道进行正常服务。也有的团队采用双写mq双消费的方式来保证这个过程，也是被逼急了：）。如果kafka死掉了，业务会切换到备用通道进行消费。

## 扩展自己的HA

如果你不想用redis，比如你先要用hbase，那也是很简单的。
但需要实现一个HA接口。
```
public interface HA {
    void close();

    void configure(Properties properties);

    void preSend(Packet packet) throws HaException;

    void postSend(Packet packet) throws HaException;

    void doRecovery(AbstractProducer producer) throws HaException;
}
```
使用之前，还需要注册一下你的插件。

```
AbstractProducer.register("log", "com.sayhiai.arch.okmq.api.producer.ha.Ha2SimpleLog");
```

## 重要参数

```
okmq.ha.recoveryPeriod 恢复线程检测周期，默认5秒

okmq.redis.mode redis的集群模式，可选:single、sentinel、cluster
okmq.redis.endpoint 地址，多个地址以,分隔
okmq.redis.connectionTimeout 连接超时
okmq.redis.soTimeout socket超时
okmq.redis.lockPx 分布式锁的持有时间，可默认，5min
okmq.redis.splitMillis 间隔时间，redis换一个key进行运算，默认5min
okmq.redis.poolConfig.* 兼容jedis的所有参数
```
# 1.0.0 版本功能

1、进行了生产端的高可用抽象，实现了kafka的样例。

2、增加了SimpleLog的ping、pong日志实现。

3、增加了Redis的生产端备用通道。包含single、cluster、sentinel三种模式。

4、可以自定义其他备用通道。

5、兼容kakfa的所有参数设置。

## 规划

### 2.0.0

1、实现ActiveMQ的集成。

2、实现消费者的备用通道集成。

3、增加嵌入式kv存储的生产者集成。

4、更精细的控制系统的行为。

5、加入开关和预热，避免新启动mq即被压垮。

6、redis分片机制，大型系统专用。

### 3.0.0

1、监控功能添加。

2、rest接口添加。

# 使用限制

当你把参数ha设置为true，表明你已经收到以下的使用限制。反之，系统反应于原生无异。

**使用限制：**
本工具仅适用于非顺序性、非事务性的普通消息投递，且客户端已经做了幂等。一些订单系统、消息通知等业务，非常适合。如果你需要其他特性，请跳出此页面。

kafka死亡，或者redis单独死亡，消息最终都会被发出，仅当kafka与redis同时死亡，消息才会发送失败，并记录在日志文件里。

正常情况下，redis的使用容量极少极少。异常情况下，redis的容量有限，会迅速占满。redis的剩余时间就是你的`StopWatch`，你必须在这个时间内恢复你的消息系统，一定要顶住哇。

# End

系统目前处于1.0.0版本，正在线上小规模试用。工具小众，但适用于大部分应用场景。如果你正在寻求这样的解决方案，欢迎一块完善代码。

github地址：
```
https://github.com/sayhiai/okmq
```

![](https://user-gold-cdn.xitu.io/2019/5/30/16b082290b590a2c?w=891&h=489&f=png&s=376268)
