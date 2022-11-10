package spark.streaming.function;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.streaming.constants.MachineOutlierConstants;
import spark.streaming.model.predictor.Prediction;
import spark.streaming.model.scorer.DataInstanceScorer;
import spark.streaming.model.scorer.DataInstanceScorerFactory;
import spark.streaming.model.scorer.ScorePackage;
import spark.streaming.util.Configuration;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author luandopke
 */
public class SSObservationScore extends BaseFunction implements FlatMapFunction<Row, Row> {
    private static final Logger LOG = LoggerFactory.getLogger(SSObservationScore.class);
    private long previousTimestamp;
    private String dataTypeName;
    private DataInstanceScorer dataInstanceScorer;
    private List<Object> observationList;

    public SSObservationScore(Configuration config) {
        super(config);
        previousTimestamp = 0;
        dataTypeName = config.get(MachineOutlierConstants.Config.SCORER_DATA_TYPE);
        observationList = new ArrayList<>();
        dataInstanceScorer = DataInstanceScorerFactory.getDataInstanceScorer(dataTypeName);
    }

    @Override
    public Iterator<Row> call(Row input) throws Exception {
        super.calculateThroughput();
        List<Row> tuples = new ArrayList<>();
        long timestamp = input.getLong(1);
        //TODO: implements spark windowing method
        if (timestamp > previousTimestamp) {
            // a new batch of observation, calculate the scores of old batch and then emit
            if (!observationList.isEmpty()) {
                List<ScorePackage> scorePackageList = dataInstanceScorer.getScores(observationList);
                for (ScorePackage scorePackage : scorePackageList) {
                    tuples.add(RowFactory.create(scorePackage.getId(), scorePackage.getScore(), previousTimestamp, scorePackage.getObj(), input.get(input.size() - 1)));
                }
                observationList.clear();
            }
            previousTimestamp = timestamp;
        }
        observationList.add(input.get(2));
        return tuples.iterator();
    }
}