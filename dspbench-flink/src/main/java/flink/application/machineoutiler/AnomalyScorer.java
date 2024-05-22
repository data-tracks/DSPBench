package flink.application.machineoutiler;

import flink.constants.MachineOutlierConstants;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class AnomalyScorer extends Metrics implements
        FlatMapFunction<Tuple4<String, Double, Long, Object>, Tuple5<String, Double, Long, Object, Double>> {

    private static final Logger LOG = LoggerFactory.getLogger(AnomalyScorer.class);

    private static Map<String, Queue<Double>> slidingWindowMap;
    private static int windowLength;
    private static long previousTimestamp;

    Configuration config;

    public AnomalyScorer(Configuration config) {
        super.initialize(config);
        this.config = config;
        //config.getString(MachineOutlierConstants.Conf.SCORER_DATA_TYPE, "machineMetadata");
        windowLength = config.getInteger(MachineOutlierConstants.Conf.ANOMALY_SCORER_WINDOW_LENGTH, 10);
        previousTimestamp = 0;
    }

    private Map<String, Queue<Double>> getWindow() {
        if (slidingWindowMap == null) {
            slidingWindowMap = new HashMap<>();
        }

        return slidingWindowMap;
    }

    @Override
    public void flatMap(Tuple4<String, Double, Long, Object> input,
            Collector<Tuple5<String, Double, Long, Object, Double>> out) {
        super.initialize(config);
        super.incBoth();
        getWindow();
        long timestamp = input.getField(2);
        String id = input.getField(0);
        double dataInstanceAnomalyScore = input.getField(1);

        Queue<Double> slidingWindow = slidingWindowMap.get(id);
        if (slidingWindow == null) {
            slidingWindow = new LinkedList<>();
        }

        // update sliding window
        slidingWindow.add(dataInstanceAnomalyScore);
        if (slidingWindow.size() > windowLength) {
            slidingWindow.poll();
        }
        slidingWindowMap.put(id, slidingWindow);

        double sumScore = 0.0;
        for (double score : slidingWindow) {
            sumScore += score;
        }

        out.collect(new Tuple5<String, Double, Long, Object, Double>(id, sumScore, timestamp, input.getField(3),
                dataInstanceAnomalyScore));
    }
}
