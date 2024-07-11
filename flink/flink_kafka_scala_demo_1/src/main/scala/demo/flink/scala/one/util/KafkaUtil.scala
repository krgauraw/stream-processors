package demo.flink.scala.one.util


import demo.flink.scala.one.config.AppConfig
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import java.util.Properties

class KafkaUtil(config: AppConfig) {

  def kafkaConsumerProperties(): Properties = {
    val prop = new Properties
    prop.setProperty("bootstrap.servers", config.getString("kafka.broker-servers", "localhost:9092"))
    prop.setProperty("group.id", config.getString("kafka.groupId", ""))
    // isolation.level configuration that determines how the consumer interacts with the Topic's partitions.
    // It ensures data consistency and prevents data loss by controlling the visibility of committed offsets.
    // There are four isolation levels available: read_committed , read_uncommitted, repeatable_read, snapshot
    prop.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    // possible value: earliest, latest, none
    // earliest: Resets the offset to the earliest available offset for the topic-partition.
    // latest: Resets the offset to the latest available offset for the topic-partition.
    // none: Throw an exception if there is no existing offset for the topic-partition.
    val kafkaOffsetReset = config.getString ("kafka.auto.offset.reset", "earliest")
    prop.setProperty("auto.offset.reset", kafkaOffsetReset)
    prop
  }

  def kafkaProducerProperties(): Properties = {
    val prop = new Properties
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.broker-servers", ""))
    // The linger.ms configuration in Kafka's producer settings refers to the maximum amount of time (in milliseconds)
    // the producer will wait for multiple acknowledgments from the broker before considering the ProduceRequest to be complete.
    // This timeout is used to balance the trade-off between latency and throughput.
    // A higher linger.ms value can result in lower latency, but may also lead to a higher memory usage and potentially longer delays in processing messages.
    // Conversely, a lower linger.ms value might reduce memory usage but could increase latency.
    // It's essential to set this value based on your production environment's requirements.
    prop.put(ProducerConfig.LINGER_MS_CONFIG, new Integer(10))
    // batch.size determines the maximum number of messages to be sent in a single batch to the broker. default is: 16384
    // A higher batch size can be beneficial for high-throughput applications,
    // while a lower batch size may be more suitable for low-latency applications.
    prop.put(ProducerConfig.BATCH_SIZE_CONFIG, new Integer(16384 * 4))
    //Valid values for compression.type include none, gzip, snappy, and lz4.
    // The default value is none, which means that no compression is applied.
    // By specifying a compression algorithm, you can optimize your Kafka producer for better performance and scalability.
    prop.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    prop
  }

  def getStringKafkaConsumer(): FlinkKafkaConsumer[String] = {
    val consumer = new FlinkKafkaConsumer[String](config.getString("kafka.input.topic", ""), new SimpleStringSchema, kafkaConsumerProperties)
    consumer
  }

  def getStringKafkaProducer(): FlinkKafkaProducer[String] = {
    val producer = new FlinkKafkaProducer[String](config.getString("kafka.output.topic", ""), new SimpleStringSchema, kafkaProducerProperties())
    producer
  }

  def kafkaStringSink(kafkaTopic: String): SinkFunction[String] = {
    new FlinkKafkaProducer[String](kafkaTopic, new SimpleStringSchema, kafkaProducerProperties())
  }

}
