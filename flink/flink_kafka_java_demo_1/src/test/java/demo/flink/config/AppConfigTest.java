package demo.flink.config;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class AppConfigTest {

    @Test
    public void testGetString() {
        String s = AppConfig.getString("kafka.zookeeper", "localhost:2181");
        assertEquals("localhost:2181", s);
    }
}
