package demo.flink.scala.one.task

import com.typesafe.config.ConfigFactory
import demo.flink.scala.one.config.AppConfig
import demo.flink.scala.one.function.StringMessageProcessFunction
import demo.flink.scala.one.util.{FlinkUtil, KafkaUtil}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool

import java.io.File

class DemoStreamTask(config: AppConfig, kafkaUtil: KafkaUtil, flinkUtil: FlinkUtil) {

  def process(): Unit = {
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
    val inputConsumerName = config.getString("kafka.input.consumer_name", "")
    val kafkaConsumerParallelism = config.getConfig().getInt("kafka.consumer.parallelism")
    val jobName = config.getString("job.name", "")

    val env = flinkUtil.getExecutionContext()
    val consumer = kafkaUtil.getStringKafkaConsumer()

    val processStreamTask = env.addSource(consumer).name(inputConsumerName).uid(inputConsumerName)
      .setParallelism(kafkaConsumerParallelism).rebalance.process(new StringMessageProcessFunction(config))
      .name("string-message-processor").uid("string-message-processor")

    // Process Side Output
    processStreamTask.getSideOutput(config.messageTransmitterTag).addSink(kafkaUtil.kafkaStringSink(config.getString("kafka.output.topic","")))

    env.execute(jobName)
  }
}

object DemoStreamTask {
  def main(args: Array[String]): Unit = {
    println("Hello Flink!")
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("application.conf").withFallback(ConfigFactory.systemEnvironment()))
    val jobConfig = new AppConfig(config)
    val kafkaUtil = new KafkaUtil(jobConfig)
    val flinkUtil = new FlinkUtil(jobConfig)
    val task = new DemoStreamTask(jobConfig, kafkaUtil, flinkUtil)
    task.process()
  }
}
