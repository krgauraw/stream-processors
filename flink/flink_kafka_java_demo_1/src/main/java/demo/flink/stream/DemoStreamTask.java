package demo.flink.stream;

import com.typesafe.config.Config;
import demo.flink.function.StringMessageProcessFunction;
import demo.flink.util.FlinkUtil;
import demo.flink.util.KafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class DemoStreamTask {

    public void process(Config jobConfig, FlinkUtil flinkUtil, KafkaUtil kafkaUtil) throws Exception {
        String inputConsumerName = jobConfig.getString("kafka.input.consumer_name");
        Integer kafkaConsumerParallelism = jobConfig.getInt("kafka.consumer.parallelism");
        String jobName = jobConfig.getString("job.name");

        StreamExecutionEnvironment env = flinkUtil.getExecutionContext(jobConfig);
        FlinkKafkaConsumer<String> consumer = kafkaUtil.getStringKafkaConsumer(jobConfig, kafkaUtil.kafkaConsumerProperties(jobConfig));

        DataStream<String> processStreamTask = env.addSource(consumer).name(inputConsumerName)
                .uid(inputConsumerName).setParallelism(kafkaConsumerParallelism).rebalance().
                process(new StringMessageProcessFunction(jobConfig))
                .name("string-message-processor").uid("string-message-processor");

        env.execute(jobName);
    }
}
