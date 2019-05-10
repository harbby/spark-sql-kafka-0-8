## spark-sql-kafka-0-8

Spark Structured Streaming kafka-0-8

## maven
```xml
<dependency>
  <groupId>com.github.harbby</groupId>
  <artifactId>spark-sql-kafka-0-8</artifactId>
  <version>1.0.0-alpha2</version>
</dependency>
```

## limit
* must spark2.3+
* must writeStream().trigger(Trigger.Continuous...)

### Use
```
val sparkSession = ...

val kafka08:DataFrame = sparkSession.readStream()
    .format("kafka08")
    .option("topics", "topic1,topic2")
    .option("bootstrap.servers", "broker1:9092,broker2:9092")
    .option("group.id", "test1")
    .option("auto.offset.reset", "largest")  //largest or smallest
    .option("zookeeper.connect", "zk1:2181,zk2:2181")
    .option("auto.commit.enable", "true")
    .option("auto.commit.interval.ms", "5000")
    .load();
    
    ...
    
    dataFrame.writeStream()
         .trigger(Trigger.Continuous(Duration.apply(90, TimeUnit.SECONDS))) //it is necessary
         ...   
```