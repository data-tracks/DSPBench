package flink.application.smartgrid;

import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.application.smartgrid.window.SlidingWindow;
import flink.application.smartgrid.window.SlidingWindowCallback;
import flink.application.smartgrid.window.SlidingWindowEntry;
import flink.constants.SentimentAnalysisConstants;
import flink.constants.SmartGridConstants;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

public class SlideWindow implements FlatMapFunction<Tuple8<String, Long, Double, Integer, String, String, String, String>, Tuple7<Long, String, String, String, Double, Integer, String>> {

    private static final Logger LOG = LoggerFactory.getLogger(SlideWindow.class);

    private static SlidingWindow window;

    public SlideWindow(Configuration config) {
        createWindow();
    }

    private SlidingWindow createWindow(){
        if (window == null) {
            window =new SlidingWindow(1 * 60 * 60);
        }

        return window;
    }

    @Override
    public void flatMap(Tuple8<String, Long, Double, Integer, String, String, String, String> input, Collector<Tuple7<Long, String, String, String, Double, Integer, String>> out) {
        createWindow();
        int type = input.getField(3);

        // we are interested only in load
        if (type == SmartGridConstants.Measurement.WORK) {
            return;
        }

        SlidingWindowEntryImpl windowEntry = new SlidingWindowEntryImpl(
                input.getField(1), input.getField(2),
                input.getField(6), input.getField(5),
                input.getField(4));

        window.add(windowEntry, new SlidingWindowCallback() {
            @Override
            public void remove(List<SlidingWindowEntry> entries) {
                for (SlidingWindowEntry e : entries) {
                    SlidingWindowEntryImpl entry = (SlidingWindowEntryImpl) e;
                    out.collect(new Tuple7<Long, String, String, String, Double, Integer, String>(entry.ts, entry.houseId, entry.houseHoldId, entry.plugId, entry.value, -1, input.f7));
                }
            }
        });

        out.collect(new Tuple7<Long, String, String, String, Double, Integer, String>(windowEntry.ts, windowEntry.houseId, windowEntry.houseHoldId, windowEntry.plugId, windowEntry.value, 1, input.f7));
        //super.calculateThroughput();
    }

    private class SlidingWindowEntryImpl implements SlidingWindowEntry {
        private String houseId;
        private String houseHoldId;
        private String plugId;
        private long ts;
        private double value;

        private SlidingWindowEntryImpl(long ts, double value, String houseId, String houseHoldId, String plugId) {
            this.ts = ts;
            this.value = value;
            this.houseId = houseId;
            this.houseHoldId = houseHoldId;
            this.plugId = plugId;
        }

        @Override
        public long getTime() {
            return ts;
        }
    }
}
