package demo.flink.stream;

import com.typesafe.config.Config;
import demo.flink.config.AppConfig;
import demo.flink.util.FlinkUtil;
import demo.flink.util.KafkaUtil;

// Driver Program, which invoke flink implementation class
public class TriggerStreamTask {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello Flink!");
        Config jobConfig = AppConfig.config;
        FlinkUtil flinKUtil = new FlinkUtil();
        KafkaUtil kafkaUtil = new KafkaUtil();
        DemoStreamTask task = new DemoStreamTask();
        task.process(jobConfig, flinKUtil, kafkaUtil);
    }
}
