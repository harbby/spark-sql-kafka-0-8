## spark-sql-kafka-0-8

Spark Structured Streaming kafka source 

support kafka-0.8.2.1+  kafka-0.9

## License
Apache License, Version 2.0 http://www.apache.org/licenses/LICENSE-2.0

## maven
```xml
<dependency>
  <groupId>com.github.harbby</groupId>
  <artifactId>spark-sql-kafka-0-8</artifactId>
  <version>1.0.0</version>
</dependency>
```

## limit
* must spark2.3+
* must writeStream().trigger(Trigger.Continuous...)

### Use
+ create
```
val sparkSession = ...

val kafka:DataFrame = sparkSession.readStream()
    .format("kafka08")
    .option("topics", "topic1,topic2")
    .option("bootstrap.servers", "broker1:9092,broker2:9092")
    .option("group.id", "test1")
    .option("auto.offset.reset", "largest")  //largest or smallest
    .option("zookeeper.connect", "zk1:2181,zk2:2181")
    .option("auto.commit.enable", "true")
    .option("auto.commit.interval.ms", "5000")
    .load(); 
```
+ schema
```
kafka.printSchema();

root
 |-- _key: binary (nullable = true)
 |-- _message: binary (nullable = true)
 |-- _topic: string (nullable = false)
 |-- _partition: integer (nullable = false)
 |-- _offset: long (nullable = false)
```

+ sink
```
    dataFrame.writeStream()
         .trigger(Trigger.Continuous(Duration.apply(90, TimeUnit.SECONDS))) //it is necessary
         ...  
```