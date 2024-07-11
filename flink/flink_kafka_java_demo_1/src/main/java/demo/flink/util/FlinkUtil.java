package demo.flink.util;

import com.typesafe.config.Config;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkUtil {
    public  StreamExecutionEnvironment getExecutionContext(Config config) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setUseSnapshotCompression(config.getBoolean("task.checkpointing.compressed"));
        env.enableCheckpointing(config.getInt("task.checkpointing.interval"));
        env.getCheckpointConfig().setCheckpointTimeout(config.getLong("task.checkpointing.timeout"));
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(config.getInt("task.restart-strategy.attempts"), config.getLong("task.restart-strategy.delay")));
        return env;
    }
}
