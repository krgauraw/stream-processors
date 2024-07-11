package demo.flink.scala.one.util


import demo.flink.scala.one.config.AppConfig
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

class FlinkUtil(config: AppConfig) {

  def getExecutionContext(): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setUseSnapshotCompression(config.getBoolean("task.checkpointing.compressed", false))
    env.enableCheckpointing(config.getConfig().getInt("task.checkpointing.interval"))
    env.getCheckpointConfig.setCheckpointTimeout(config.getConfig().getLong("task.checkpointing.timeout"))
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.getConfig().getInt("task.restart-strategy.attempts"), config.getConfig().getLong("task.restart-strategy.delay")))
    env
  }
}
