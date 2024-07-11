package demo.flink.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class AppConfig {

    private static Config defaultConf = ConfigFactory.load();
    private static Config envConf = ConfigFactory.systemEnvironment();
    public static Config config = envConf.withFallback(defaultConf);

    public static String getString(String key, String defValue) {
        return config.hasPath(key) ? config.getString(key) : defValue;
    }



}
