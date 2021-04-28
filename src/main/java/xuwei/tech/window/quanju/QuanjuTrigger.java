package xuwei.tech.window.quanju;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class QuanjuTrigger extends Trigger<Tuple2<String, Long>, GlobalWindow> {

    private static final long serialVersionUID = 1L;

    private static int flag = 0;

    @Override
    public TriggerResult onElement(Tuple2<String, Long> element, long timestamp, GlobalWindow window, TriggerContext ctx) {
        ctx.registerProcessingTimeTimer(ctx.getCurrentProcessingTime() + 10000);
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
        ctx.deleteProcessingTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(GlobalWindow window,
                        OnMergeContext ctx) {
// only register a timer if the time is not yet past the end of the merged window
// this is in line with the logic in onElement(). If the time is past the end of
// the window onElement() will fire and setting a timer here would fire the window twice.
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
            ctx.registerProcessingTimeTimer(windowMaxTimestamp);
        }
    }

    @Override
    public String toString() {
        return "ProcessingTimeTrigger()";
    }

}
