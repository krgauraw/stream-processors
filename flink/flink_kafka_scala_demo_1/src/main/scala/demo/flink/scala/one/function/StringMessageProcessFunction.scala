package demo.flink.scala.one.function


import demo.flink.scala.one.config.AppConfig
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector

class StringMessageProcessFunction(config: AppConfig)(implicit val stringTypeInfo: TypeInformation[String]) extends ProcessFunction[String, String]{
  override def processElement(i: String, context: ProcessFunction[String, String]#Context, collector: Collector[String]): Unit = {
    println("kafka message received : "+ i)
    context.output(config.messageTransmitterTag, i.toUpperCase)
    println("Message forwarded to output topic after processing.")
  }
}
