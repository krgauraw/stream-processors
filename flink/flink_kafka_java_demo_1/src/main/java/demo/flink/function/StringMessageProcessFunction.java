package demo.flink.function;

import com.typesafe.config.Config;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class StringMessageProcessFunction extends ProcessFunction<String, String> implements Serializable {

    private Config jobConfig;

    private Logger logger = LoggerFactory.getLogger(StringMessageProcessFunction.class);

    public StringMessageProcessFunction(Config jobConfig){
        this.jobConfig = jobConfig;
    }
    @Override
    public void processElement(String s, ProcessFunction<String, String>.Context context, Collector<String> collector) throws Exception {
        System.out.println("StringMessageProcessFunction :: processElement() :: start");
        logger.info("Message Received From Kafka: " + s);
        System.out.println("Message Received From Kafka: " + s);
        System.out.println("StringMessageProcessFunction :: processElement() :: end");
    }
}
