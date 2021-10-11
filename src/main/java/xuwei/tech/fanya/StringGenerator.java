package xuwei.tech.fanya;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalTime;

/**
 * Simple source class which produces an integer every second
 *
 * @author preetdeep.kumar
 */
@SuppressWarnings("serial")
public class StringGenerator implements SourceFunction<String> {
    volatile boolean isRunning = true;
    final Logger logger = LoggerFactory.getLogger(StringGenerator.class);

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        int i = 0;
        String counter = "aaaaaaa";

        while (isRunning) {
            i++;
            ctx.collect(counter+i);
            logger.info("Produced Integer value {} at {}", counter+i, LocalTime.now());
//            Thread.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}