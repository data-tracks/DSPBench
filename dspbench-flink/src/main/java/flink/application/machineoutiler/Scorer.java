package flink.application.machineoutiler;

import flink.application.logprocessing.GeoStats;
import flink.application.sentimentanalysis.sentiment.SentimentClassifier;
import flink.application.sentimentanalysis.sentiment.SentimentClassifierFactory;
import flink.application.sentimentanalysis.sentiment.SentimentResult;
import flink.constants.MachineOutlierConstants;
import flink.constants.SentimentAnalysisConstants;
import flink.util.Metrics;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class Scorer extends Metrics implements FlatMapFunction<Tuple4<String, Long, MachineMetadata, String>, Tuple5<String, Double, Long, Object,String>> {

    private static final Logger LOG = LoggerFactory.getLogger(Scorer.class);

    private static long previousTimestamp;
    private static String dataTypeName;
    private static List<Object> observationList;
    private static DataInstanceScorer dataInstanceScorer;
    Configuration config;

    public Scorer(Configuration config) {
        super.initialize(config);
        previousTimestamp = 0;
        this.config = config;
        createScorer(config);
        getList();

    }

    private  List<Object> getList() {
        if (observationList == null) {
            observationList = new ArrayList<>();
        }

        return observationList;
    }

    private DataInstanceScorer createScorer(Configuration config) {
        if (dataInstanceScorer == null) {
            dataTypeName = config.getString(MachineOutlierConstants.Conf.SCORER_DATA_TYPE, "machineMetadata");
            dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
        }

        return dataInstanceScorer;
    }

    @Override
    public void flatMap(Tuple4<String, Long, MachineMetadata, String> input, Collector<Tuple5<String, Double, Long, Object,String>> out) {
        super.initialize(config);
        getList();
        createScorer(config);
        long timestamp = input.getField(1);
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    out.collect(new Tuple5<String, Double, Long, Object, String>(scorePackage.getId(), scorePackage.getScore(), previousTimestamp, scorePackage.getObj(), input.f3));
                }
                observationList.clear();
            }

            previousTimestamp = timestamp;
        }

        observationList.add(input.getField(2));
        super.calculateThroughput();
    }
}
