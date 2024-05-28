package flink.application.trendingtopics;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.application.adanalytics.AdEvent;
import flink.application.adanalytics.RollingCTR;
import flink.constants.BaseConstants;
import flink.constants.TrendingTopicsConstants;
import flink.tools.NthLastModifiedTimeTracker;
import flink.tools.SlidingWindowCounter;
import flink.util.Metrics;

public class RollingCount extends Metrics implements WindowFunction<Tuple1<String>, Tuple3<Object, Long, Integer>, String, TimeWindow>{
    private static final Logger LOG = LoggerFactory.getLogger(RollingCTR.class);
    Configuration config;

    private static final String WINDOW_LENGTH_WARNING_TEMPLATE =
        "Actual window length is %d seconds when it should be %d seconds"
            + " (you can safely ignore this warning during the startup phase)";

    private SlidingWindowCounter<Object> counter;
    private int windowLengthInSeconds;
    private int emitFrequencyInSeconds;
    private NthLastModifiedTimeTracker lastModifiedTracker;

    public RollingCount(Configuration config) {
        this(config, 60);
    }
    
    public RollingCount(Configuration config, int emitFrequencyInSeconds) {
        super.initialize(config);
        this.config = config;
        this.emitFrequencyInSeconds = emitFrequencyInSeconds;

        windowLengthInSeconds = config.getInteger(TrendingTopicsConstants.Conf.COUNTER_WINDOW, 300);
        
        int numChunks = windowLengthInSeconds/emitFrequencyInSeconds;
        
        counter = new SlidingWindowCounter<>(numChunks);
        lastModifiedTracker = new NthLastModifiedTimeTracker(numChunks);
    }

    @Override
    public void apply(String key, TimeWindow window, Iterable<Tuple1<String>> input,
            Collector<Tuple3<Object, Long, Integer>> out) throws Exception {
        super.initialize(config);

        for(Tuple1<String> in : input){
            super.incReceived();

            String obj = in.getField(0);
            counter.incrementCount(obj);
        }

        Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
        int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
        lastModifiedTracker.markAsModified();
        
        if (actualWindowLengthInSeconds != windowLengthInSeconds) {
            LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
        }

        for (Entry<Object, Long> entry : counts.entrySet()) {
            Object obj = entry.getKey();
            Long count = entry.getValue();
            //collector.emit(new Values(obj, count, actualWindowLengthInSeconds));
            super.incEmitted();
            out.collect(new Tuple3<Object,Long,Integer>(obj, count, actualWindowLengthInSeconds));
        }
    }
}
