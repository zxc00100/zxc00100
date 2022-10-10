Author：gaozhy

Blog:http://www.gaozhy.cn

CSDN: https://blog.csdn.net/qq_31871785

## Kafka Streaming概述

Kafka Streams是一个用于构建应用程序和微服务的**客户端库**，其中的输入和输出数据存储在Kafka集群中。它结合了在客户端编写和部署标准Java和Scala应用程序的简单性，以及Kafka服务器端集群技术的优点。

### 特点
1. 弹性、高可扩展、容错
2. 可以部署在容器、虚拟机、单独、云环境中
3. 同样适用于小型、中型和大型用例
4. 集成Kafka Security
5. 写标准的JAVA和Scala应用
6. 精确一次处理语义
7. 无需单独的处理群集
8. 支持多种开发平台

### 核心概念
**Topology（）**：表示一个流计算任务，等价于MapReduce中的job。不同的是MapReduce的job作业最终会停止，但是Topology会一直运行在内存中，除非人工关闭该Topology

**Stream**：它代表了一个无限的，不断更新的Record数据集。流是有序，可重放和容错的不可变数据记录序列，其中数据记录被定义为键值对

**States**：用以持久化存放流计算状态结果，可以用以容错和故障恢复

**Time**：Event time(事件时间)、Processing time(处理时间)、Ingestion time(摄入时间)

**所谓的流处理就是通过Topology编织程序对Stream中Record元素的处理的逻辑/流程。**


### 架构
Kafka Streams通过构建Kafka生产者和消费者库并利用Kafka的本机功能来提供数据并行性，分布式协调，容错和操作简便性，从而简化了应用程序开发。

![image](http://kafka.apache.org/23/images/streams-architecture-overview.jpg)

Kafka的消息分区用于存储和传递消息， Kafka Streams对数据进行分区以进行处理。 Kafka Streams使用Partition和Task的概念作为基于Kafka Topic分区的并行模型的逻辑单元。在并行化的背景下，Kafka Streams和Kafka之间有着密切的联系：
1. 每个stream分区都是完全有序的数据记录序列，并映射到Kafka Topic分区。
2. Stream中的数据记录映射到该Topic的Kafka消息。
3. 数据记录的key决定了Kafka和Kafka Streams中数据的分区，即数据如何路由到Topic的特定分区。

#### 任务的并行度
Kafka Streams基于应用程序的输入流分区创建固定数量的Task，每个任务(Task)分配来自输入流的分区列表（即Kafka主题）。分区到任务的分配永远不会改变，因此每个任务都是应用程序的固定平行单元。然后，任务可以根据分配的分区实例化自己的处理器拓扑; 它们还为每个分配的分区维护一个缓冲区，并从这些记录缓冲区一次一个地处理消息。因此，流任务可以独立并行地处理，无需人工干预。

![](http://kafka.apache.org/23/images/streams-architecture-tasks.jpg)

用户可以启动多个KafkaStream实例，这样等价启动了多个Stream Tread，每个Thread处理1~n个Task。一个Task对应一个分区，因此Kafka Stream流处理的并行度不会超越Topic的分区数。需要值得注意的是Kafka的每个Task都维护这自身的一些状态，线程之间不存在状态共享和通信。因此Kafka在实现流处理的过程中扩展是非常高效的。

![](http://kafka.apache.org/23/images/streams-architecture-threads.jpg)

#### 容错
Kafka Streams构建于Kafka本地集成的容错功能之上。 Kafka分区具有高可用性和复制性;因此当流数据持久保存到Kafka时，即使应用程序失败并需要重新处理它也可用。 Kafka Streams中的任务利用Kafka消费者客户端提供的容错功能来处理故障。如果任务运行的计算机故障了，Kafka Streams会自动在其余一个正在运行的应用程序实例中重新启动该任务。

此外，Kafka Streams还确保local state store也很有力处理故障容错。对于每个state store，Kafka Stream维护一个带有副本changelog的Topic，在该Topic中跟踪任何状态更新。这些changelog Topic也是分区的，该分区和Task是一一对应的。如果Task在运行失败并Kafka Stream会在另一台计算机上重新启动该任务，Kafka Streams会保证在重新启动对新启动的任务的处理之前，通过重播相应的更改日志主题，将其关联的状态存储恢复到故障之前的内容。


## 实战篇
> 注：创建Kafka Streaming Topology有两种方式
> - low-level：Processor API
> - high-level：Kafka Streams DSL(DSL：提供了通用的数据操作算子，如：map, filter, join, and aggregations等)
### low-level：Processor API
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>2.2.0</version>
</dependency>
````

- 编写stream应用
```java
package lowlevel;

import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class WortCountWithProcessorAPI {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "gaozhy:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-processor-application");

        // 创建Topology
        Topology topology = new Topology();
        topology.addSource("input", "input");
        topology.addProcessor("wordCountProcessor", () -> new WordCountProcessor(), "input");
        topology.addSink("output", "output", new StringSerializer(), new LongSerializer(), "wordCountProcessor");

        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();
    }
}
```
- 编写自定义Processor
```java
package lowlevel;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;

import java.time.Duration;
import java.util.HashMap;

public class WordCountProcessor implements Processor<String, String> {
    private HashMap<String, Long> wordPair;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        wordPair = new HashMap<>();
        this.context = context;
        // 每隔1秒将处理的结果向下游传递
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
            System.out.println("----------- " + timestamp + " ----------- ");
            wordPair.forEach((k,v) -> {
                System.out.println(k +" | "+v);
                this.context.forward(k,v);
            });
        });
    }

    @Override
    public void process(String key, String value) {
        String[] words = value.split(" ");
        for (String word : words) {
            Long num = wordPair.getOrDefault(word, 0L);
            num++;
            wordPair.put(word, num);
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}
```
- 测试

> 上面案列存在的问题：
> 1. 宕机则计算的状态丢失
> 2. 并没有考虑状态中keys的数目，一旦数目过大，会导致流计算服务内存溢出。

- 状态存储

配置状态存储
```java
// 创建state，存放状态信息
Map<String, String> changelogConfig = new HashMap();
// override min.insync.replicas
changelogConfig.put("min.insyc.replicas", "1");
changelogConfig.put("cleanup.policy","compact");
StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
    Stores.persistentKeyValueStore("Counts"),
    Serdes.String(),
    Serdes.Long()).withLoggingEnabled(changelogConfig);
```
> 事实StateStore本质是一个Topic，但是改topic的清除策略不在是delete，而是compact.

关联StateStore和Processor
```java
// 创建Topology
Topology topology = new Topology();
topology.addSource("input", "input");
topology.addProcessor("wordCountProcessor", () -> new WordCountProcessor(), "input");
// 创建state，存放状态信息
Map<String, String> changelogConfig = new HashMap();
// override min.insync.replicas
changelogConfig.put("min.insyc.replicas", "1");
changelogConfig.put("cleanup.policy","compact");
StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier = Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("Counts"),
        Serdes.String(),
        Serdes.Long()).withLoggingEnabled(changelogConfig);

topology.addStateStore(countStoreSupplier,"wordCountProcessor");
topology.addSink("output", "output", new StringSerializer(), new LongSerializer(), "wordCountProcessor");

```
在自定义Processor实现类中使用state
```java
package lowlevel;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.HashMap;

public class WordCountProcessor implements Processor<String, String> {
    private KeyValueStore<String, Long> keyValueStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        keyValueStore = (KeyValueStore<String, Long>) context.getStateStore("Counts");
        this.context = context;
        // 定期向下游输出计算结果
        this.context.schedule(Duration.ofSeconds(1), PunctuationType.STREAM_TIME, timestamp -> {
            System.out.println("----------- " + timestamp + " ----------- ");
            KeyValueIterator<String, Long> iterator = keyValueStore.all();
            while (iterator.hasNext()) {
                KeyValue<String, Long> entry = iterator.next();
                this.context.forward(entry.key, entry.value);
            }
            iterator.close();
        });
    }

    @Override
    public void process(String key, String value) {
        String[] words = value.split(" ");
        for (String word : words) {
            Long oldValue = keyValueStore.get(word);
            if (oldValue == null) {
                keyValueStore.put(word, 1L);
            } else {
                keyValueStore.put(word, oldValue + 1L);
            }
        }
        context.commit();
    }

    @Override
    public void close() {

    }
}
```




### high-level：Kafka Streams DSL(重点)
Kafka Streams DSL（Domain Specific Language）构建于Streams Processor API之上。它是大多数用户推荐的，特别是初学者。大多数数据处理操作只能用几行DSL代码表示。在 Kafka Streams DSL 中有这么几个概念`KTable`、`KStream`和`GlobalKTable`

KStream是一个数据流，可以认为所有记录都通过Insert only的方式插入进这个数据流里。而KTable代表一个完整的数据集，可以理解为数据库中的表。由于每条记录都是Key-Value对，这里可以将Key理解为数据库中的Primary Key，而Value可以理解为一行记录。可以认为KTable中的数据都是通过Update only的方式进入的。也就意味着，如果KTable对应的Topic中新进入的数据的Key已经存在，那么从KTable只会取出同一Key对应的最后一条数据，相当于新的数据更新了旧的数据。

以下图为例，假设有一个KStream和KTable，基于同一个Topic创建，并且该Topic中包含如下图所示5条数据。此时遍历KStream将得到与Topic内数据完全一样的所有5条数据，且顺序不变。而此时遍历KTable时，因为这5条记录中有3个不同的Key，所以将得到3条记录，每个Key对应最新的值，并且这三条数据之间的顺序与原来在Topic中的顺序保持一致。这一点与Kafka的日志compact相同。

![](https://images2018.cnblogs.com/blog/963903/201808/963903-20180823012822162-142241598.png)

此时如果对该KStream和KTable分别基于key做Group，对Value进行Sum，得到的结果将会不同。对KStream的计算结果是<Jack，4>，<Lily，7>，<Mike，4>。而对Ktable的计算结果是<Mike，4>，<Jack，3>，<Lily，5>。

**GlobalKTable**:和KTable类似，不同点在于KTable只能表示一个分区的信息，但是GlobalKTable表示的是全局的状态信息。



> 基于DSL风格的WordCount

#### pom.xml
```xml
<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>kafka-streams</artifactId>
    <version>2.2.0</version>
</dependency>
```

#### WordCountAppliction
```java
package lowlevel;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

public class WordCountAppliction {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "gaozhy:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");

        // 流处理构建对象
        StreamsBuilder builder = new StreamsBuilder();
        // input stream
        KStream<String, String> textLines = builder.stream("input");

        // 流处理
        // Hello World
        // k: Hello World v: Hello
        // k: Hello World v: World
        KTable<String, Long> wordCount = textLines.flatMapValues(v -> Arrays.asList(v.split(" ")))
                // hello hello
                // world word
                .groupBy((k, v) -> v)
                // hello 1
                // ....
                .count();

        // output stream
        wordCount.toStream().to("output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        // 启动
        kafkaStreams.start();
    }
}
```
#### 创建Topic——Producer Input和Consumer output
```shell
 gaozhy@MacBook  ~/app/kafka_2.11-2.0.0  bin/kafka-topics.sh --create --topic input --zookeeper gaozhy:2181 --partitions 1 --replication-factor 1
Created topic "input".
 gaozhy@MacBook  ~/app/kafka_2.11-2.0.0  bin/kafka-topics.sh --create --topic output --zookeeper gaozhy:2181 --partitions 1 --replication-factor 1
Created topic "output".
```
#### 运行stream application

`右键run->WordCountAppliction main()`

#### Producer生产数据作为stream application的输入
```
 gaozhy@MacBook  ~/app/kafka_2.11-2.0.0  bin/kafka-console-producer.sh --broker-list gaozhy:9092 --topic input
>Hello World
>Hello Spark
>Hello Scala
>
```

#### 从Consumer获取stream application的处理结果
```
bin/kafka-console-consumer.sh --bootstrap-server gaozhy:9092 \
    --topic output \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
Hello	1
World	1
Spark	1
Hello	3
Scala	1
```

#### 结果剖析(略)
第一条record：`all streams lead to kafka`
![图1](http://kafka.apache.org/23/images/streams-table-updates-01.png)

第二条record：`hello kafka streams`
![图2](http://kafka.apache.org/23/images/streams-table-updates-02.png)

```java
// 打印kafka streaming拓扑关系图
System.out.println(builder.build().describe());

Topologies:
   Sub-topology: 0
    Source: KSTREAM-SOURCE-0000000000 (topics: [input])
      --> KSTREAM-FLATMAPVALUES-0000000001
    Processor: KSTREAM-FLATMAPVALUES-0000000001 (stores: [])
      --> KSTREAM-KEY-SELECT-0000000002
      <-- KSTREAM-SOURCE-0000000000
    Processor: KSTREAM-KEY-SELECT-0000000002 (stores: [])
      --> KSTREAM-FILTER-0000000006
      <-- KSTREAM-FLATMAPVALUES-0000000001
    Processor: KSTREAM-FILTER-0000000006 (stores: [])
      --> KSTREAM-SINK-0000000005
      <-- KSTREAM-KEY-SELECT-0000000002
    Sink: KSTREAM-SINK-0000000005 (topic: KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition)
      <-- KSTREAM-FILTER-0000000006

  Sub-topology: 1
    Source: KSTREAM-SOURCE-0000000007 (topics: [KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition])
      --> KSTREAM-AGGREGATE-0000000004
    Processor: KSTREAM-AGGREGATE-0000000004 (stores: [KSTREAM-AGGREGATE-STATE-STORE-0000000003])
      --> KTABLE-TOSTREAM-0000000008
      <-- KSTREAM-SOURCE-0000000007
    Processor: KTABLE-TOSTREAM-0000000008 (stores: [])
      --> KSTREAM-SINK-0000000009
      <-- KSTREAM-AGGREGATE-0000000004
    Sink: KSTREAM-SINK-0000000009 (topic: output)
      <-- KTABLE-TOSTREAM-0000000008
```
> 剖析：
> 1. 在kafka streaming拓扑关系图中有两个子拓扑Sub-topology: 0和Sub-topology: 1
> 2. Sub-topology: 0的`KSTREAM-SOURCE-0000000000会`将input topic中的record作为数据源，然后经过处理器（Processor）`KSTREAM-FLATMAPVALUES-0000000001`、`KSTREAM-KEY-SELECT-0000000002`、`KSTREAM-FILTER-0000000006`（*过滤掉key为空的中间结果*）,最终将处理完成的结果存放到topic `KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition`中。**为什么这里需要`*-repartition`的topic呢？主要原因是保证在shuffle结束后key相同的record存放在`*-repartition`相同的分区中，这样就为下一步的统计做好了准备**
> 3. Sub-topology: 1的`KSTREAM-SOURCE-0000000007`将`*-repartition`topic中的record作为数据源，然后经过Processor`KSTREAM-AGGREGATE-0000000004`进行聚合操作，并且将聚合的状态信息存放大topic`KSTREAM-AGGREGATE-STATE-STORE-0000000003`中，继续经过Processor`KTABLE-TOSTREAM-0000000008`，最终将处理完成的结果存放到`output`中



#### 状态存储

```java
KTable<String, Long> wordCount = textLines.flatMapValues(v -> Arrays.asList(v.split(" ")))
                .groupBy((k, v) -> v)
                // 状态存储
                .count(Materialized.<String,Long,KeyValueStore<Bytes,byte[]>>as("counts"));
        // output stream
        wordCount.toStream().to("output", Produced.with(Serdes.String(), Serdes.Long()));
        System.out.println(builder.build().describe());
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
        // 启动
        kafkaStreams.start();
```

#### 无状态的转换算子(stateless)

- Branch

  > 分流

  ```java
  KStream<String, Long> stream = ...;
  KStream<String, Long>[] branches = stream.branch(
      (key, value) -> key.startsWith("A"), /* first predicate  */
      (key, value) -> key.startsWith("B"), /* second predicate */
      (key, value) -> true                 /* third predicate  */
    );
  
  // KStream branches[0] contains all records whose keys start with "A"
  // KStream branches[1] contains all records whose keys start with "B"
  // KStream branches[2] contains all other records
  ```

- Filter | filterNot

  > Filter： 保留符合条件的结果
  >
  > filterNot：保留不符合条件的结果

  ```java
  // 保留所有的大于零的正数
  KStream<String, Long> onlyPositives = stream.filter((key, value) -> value > 0);
  
  KStream<String, Long> onlyPositives = stream.filterNot((key, value) -> value <= 0);
  ```

- FlatMap

  > 将一条record变为多条record并且将多条记录展开

  ```java
  KStream<String, Integer> transformed = stream.flatMap(
       // Here, we generate two output records for each input record.
       // We also change the key and value types.
       // Example: (345L, "Hello") -> ("HELLO", 1000), ("hello", 9000)
      (key, value) -> {
        List<KeyValue<String, Integer>> result = new LinkedList<>();
        result.add(KeyValue.pair(value.toUpperCase(), 1000));
        result.add(KeyValue.pair(value.toLowerCase(), 9000));
        return result;
      }
    );
  ```

- FlatMapValues

  > 将一条record变为多条record并且将多条记录展开
  >
  > `(k,v) -> (k,v1),(k,v2)….`

  ```java
  KStream<byte[], String> words = sentences.flatMapValues(value -> Arrays.asList(value.split("\\s+")));
  ```

- Map | MapValues

  > 将一条record映射为另外的一条record

  ```java
  KStream<String, Integer> transformed = stream.map(
      (key, value) -> KeyValue.pair(value.toLowerCase(), value.length()));
  
  KStream<byte[], String> uppercased = stream.mapValues(value -> value.toUpperCase());
  ```

- Foreach

  > 最终操作，对每一个record进行函数处理

  ```java
  stream.foreach((key, value) -> System.out.println(key + " => " + value));
  ```

- GroupByKey | GroupBy

  > 根据Key分组或者根据指定Key分组

  ```java
  KGroupedStream<byte[], String> groupedStream = stream.groupByKey();
  
  
  KGroupedStream<String, String> groupedStream = stream.groupBy(
      (key, value) -> value,
      Grouped.with(
        Serdes.String(), /* key (note: type was modified) */
        Serdes.String())  /* value */
    );
  ```

- Merge

  > 将两个流合并为一个

  ```java
  KStream<byte[], String> stream1 = ...;
  
  KStream<byte[], String> stream2 = ...;
  
  KStream<byte[], String> merged = stream1.merge(stream2);
  ```

- Peek

  > 作为程序执行的探针，一般用于debug调试，因为peek并不会对后续的流数据带来任何影响。

  ```java
  KStream<byte[], String> unmodifiedStream = stream.peek(
      (key, value) -> System.out.println("key=" + key + ", value=" + value));
  ```

- Print

  > 最终操作，将每一个record进行输出打印

  ```java
  stream.print(Printed.toSysOut());
  // 或
  stream.print(Printed.toFile("streams.out").withLabel("streams"));
  ```

- SelectKey

  > 修改记录中key (k,v)—>(newkey,v)

  ```java
  KStream<String, String> rekeyed = stream.selectKey((key, value) -> value.split(" ")[0])
  ```

  

#### 有状态的转换算子(stateful)

有状态转换值得是每一次的处理都需要操作关联StateStore实现有状态更新。例如，在aggregating 操作中，window state store用于收集每个window的最新聚合结果。在join操作中，窗口状态存储用于收集到目前为止在定义的window边界内接收的所有记录。状态存储是容错的。如果发生故障，Kafka Streams保证在恢复处理之前完全恢复所有状态存储。

DSL中可用的有状态转换包括:

- [Aggregating](http://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-aggregating)
- [Joining](http://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-joins)
- [Windowing](http://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-windowing) (as part of aggregations and joins)
- [Applying custom processors and transformers](http://kafka.apache.org/documentation/streams/developer-guide/dsl-api.html#streams-developer-guide-dsl-process), which may be stateful, for Processor API integration

下图显示了它们之间的关系：

![](http://kafka.apache.org/23/images/streams-stateful_operations.png)

- **Aggregate**

  > 滚动聚合 按分组键聚合（非窗口化）记录的值

  ```java
  KTable<byte[], Long> aggregatedStream = groupedStream.aggregate(
      () -> 0L, /* initializer */
      (aggKey, newValue, aggValue) -> aggValue + newValue.length(), /* adder */
      Materialized.as("aggregated-stream-store") /* state store name */
          .withValueSerde(Serdes.Long()); /* serde for aggregate value */
  ```

- **Count**

  > 滚动聚合 按分组键计算记录数

  ```java
  KTable<String, Long> aggregatedStream = groupedStream.count();
  ```

- **Reduce**

  > 滚动聚合 通过分组键组合（非窗口）记录的值

  ```java
  KTable<String, Long> aggregatedStream = groupedStream.reduce(
      (aggValue, newValue) -> aggValue + newValue /* adder */);
  ```

- **注意：有状态的算子在聚合操作时的状态存储**

  ```java
  KTable<String, Integer> aggregated = groupedStream.aggregate(
      () -> 0, /* initializer */
      (aggKey, newValue, aggValue) -> aggValue + newValue, /* adder */
      Materialized.<String, Long, KeyValueStore<Bytes, byte[]>as("aggregated-stream-store" /* state store name */)
        .withKeySerde(Serdes.String()) /* key serde */
        .withValueSerde(Serdes.Integer()); /* serde for aggregate value */
  ```

  

#### Window

- Tumbling（翻滚） 固定大小 无重叠

  翻滚窗口将流元素按照固定的时间间隔，拆分成指定的窗口，窗口和窗口间元素之间没有重叠。在下图不同颜色的record表示不同的key。可以看是在时间窗口内，每个key对应一个窗口。`前闭后开`

  ![](http://kafka.apache.org/23/images/streams-time-windows-tumbling.png)

  ![](https://images2018.cnblogs.com/blog/963903/201808/963903-20180823013225516-1830552448.gif)

  

  ```java
  import org.apache.kafka.common.serialization.Serdes;
  import org.apache.kafka.common.utils.Bytes;
  import org.apache.kafka.streams.KafkaStreams;
  import org.apache.kafka.streams.KeyValue;
  import org.apache.kafka.streams.StreamsBuilder;
  import org.apache.kafka.streams.StreamsConfig;
  import org.apache.kafka.streams.kstream.*;
  import org.apache.kafka.streams.state.KeyValueStore;
  import org.apache.kafka.streams.state.WindowStore;
  
  import java.text.SimpleDateFormat;
  import java.time.Duration;
  import java.util.ArrayList;
  import java.util.Properties;
  
  public class KafkaStreamingWordCountWithWindow {
      public static void main(String[] args) {
          Properties properties = new Properties();
          properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount22");
          properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "gaozhy:9092");
          properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
          properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
  
          StreamsBuilder builder = new StreamsBuilder();
          // 流处理 数据的来源
          KStream<String, String> kStream = builder.stream("input");
  
          kStream
                  .flatMap((k, v) -> {
                      ArrayList<KeyValue<String, String>> keyValues = new ArrayList<>();
                      String[] words = v.split(" ");
                      for (String word : words) {
                          keyValues.add(new KeyValue<String, String>(k, word));
                      }
                      return keyValues;
                  })
                  .map((k, v) -> new KeyValue<String, Long>(v, 1L))
                  .groupBy((k, v) -> k, Grouped.with(Serdes.String(), Serdes.Long()))
            			// 滚动窗口的大小
                  .windowedBy(TimeWindows.of(Duration.ofSeconds(10)))
                  .reduce((value1, value2) -> value1 + value2, Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("counts").withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                  .toStream()
                  .peek(((Windowed<String> key, Long value) -> {
                      Window window = key.window();
                      SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
                      long start = window.start();
                      long end = window.end();
                      System.out.println(sdf.format(start) + " ~ " + sdf.format(end) + "\t" + key.key() + "\t" + value);
                  }));
  
          // 构建kafka streaming 应用
          KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
          kafkaStreams.start();
      }
  }
  ```

  

- Hopping （跳跃） 固定大小 有重叠

  Hopping time windows是基于时间间隔的窗口。他们模拟固定大小的（可能）重叠窗口。跳跃窗口由两个属性定义：窗口大小和其提前间隔（又名“hop”）。

  ![](http://kafka.apache.org/23/images/streams-time-windows-hopping.png)

  ![](https://images2018.cnblogs.com/blog/963903/201808/963903-20180823013127357-1860834711.gif)

  ```java
  import java.time.Duration;
  import org.apache.kafka.streams.kstream.TimeWindows;
  
  // A hopping time window with a size of 5 minutes and an advance interval of 1 minute.
  // The window's name -- the string parameter -- is used to e.g. name the backing state store.
  Duration windowSizeMs = Duration.ofMinutes(5);
  Duration advanceMs =    Duration.ofMinutes(1);
  TimeWindows.of(windowSizeMs).advanceBy(advanceMs);
  ```

  

- Sliding (滑动)  固定大小 有重合  每一个窗口至少有一个事件

  窗口只用于2个KStream进行Join计算时。该窗口的大小定义了Join两侧KStream的数据记录被认为在同一个窗口的最大时间差。假设该窗口的大小为5秒，则参与Join的2个KStream中，记录时间差小于5的记录被认为在同一个窗口中，可以进行Join计算。

  

- Session   动态 无重叠 数据驱动的窗口

  Session Window该窗口用于对Key做Group后的聚合操作中。它需要对Key做分组，然后对组内的数据根据业务需求定义一个窗口的起始点和结束点。一个典型的案例是，希望通过Session Window计算某个用户访问网站的时间。对于一个特定的用户（用Key表示）而言，当发生登录操作时，该用户（Key）的窗口即开始，当发生退出操作或者超时时，该用户（Key）的窗口即结束。窗口结束时，可计算该用户的访问时间或者点击次数等。

  Session Windows用于将基于key的事件聚合到所谓的会话中，其过程称为session化。会话表示由定义的不活动间隔（或“空闲”）分隔的活动时段。处理的任何事件都处于任何现有会话的不活动间隙内，并合并到现有会话中。如果事件超出会话间隙，则将创建新会话。会话窗口的主要应用领域是用户行为分析。基于会话的分析可以包括简单的指标.

  ![](http://kafka.apache.org/23/images/streams-session-windows-01.png)

  如果我们接收到另外三条记录（包括两条迟到的记录），那么绿色记录key的两个现有会话将合并为一个会话，从时间0开始到结束时间6，包括共有三条记录。蓝色记录key的现有会话将延长到时间5结束，共包含两个记录。最后，将在11时开始和结束蓝键的新会话。

  ![](http://kafka.apache.org/23/images/streams-session-windows-02.png)

  ```java
  import java.time.Duration;
  import org.apache.kafka.streams.kstream.SessionWindows;
  
  // A session window with an inactivity gap of 5 minutes.
  SessionWindows.with(Duration.ofMinutes(5));
  ```

  

  

  