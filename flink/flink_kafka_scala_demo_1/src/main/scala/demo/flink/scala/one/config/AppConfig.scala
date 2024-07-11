package demo.flink.scala.one.config

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag

import java.io.Serializable

class AppConfig(val config: Config) extends Serializable {

  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  def getConfig(): Config = config;
  def getString(key: String, defValue: String): String = if(config.hasPath(key)) config.getString(key) else defValue
  def getBoolean(key: String, defValue: Boolean): Boolean = if(config.hasPath(key)) config.getBoolean(key) else defValue

  val messageTransmitterTag: OutputTag[String] = OutputTag[String]("message-transmitter")
}
