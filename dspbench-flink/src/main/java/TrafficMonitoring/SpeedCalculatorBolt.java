package TrafficMonitoring;

import Constants.TrafficMonitoringConstants.Field;
import RoadModel.Road;
import Util.config.Configuration;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;

/**
 *  @author  Alessandra Fais
 *  @version Nov 2019
 *
 *  This operator uses the roadID result generated by Map-Match operator
 *  to update the average speed record of the corresponding road.
 */
public class SpeedCalculatorBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(SpeedCalculatorBolt.class);

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;

    private Map<Integer, Road> roads;

    private long t_start;
    private long t_end;
    private long processed;
    private int par_deg;

    SpeedCalculatorBolt(int p_deg) {
        par_deg = p_deg;     // bolt parallelism degree
    }

    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        LOG.info("[Calculator] Started ({} replicas).", par_deg);

        t_start = System.nanoTime(); // bolt start time in nanoseconds
        processed = 0;               // total number of processed tuples

        config = Configuration.fromMap(stormConf);
        context = topologyContext;
        collector = outputCollector;

        roads = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        int roadID = tuple.getInteger(0);   // Field.ROAD_ID
        int speed  = tuple.getInteger(1);   // Field.SPEED
        long timestamp = tuple.getLong(2);  // Field.TIMESTAMP

        LOG.debug("[Calculator] tuple: roadID " + roadID +
                ", speed " + speed +
                ", ts " + timestamp);

        int averageSpeed = 0;
        int count = 0;

        if (!roads.containsKey(roadID)) {
            Road road = new Road(roadID);
            road.addRoadSpeed(speed);
            road.setCount(1);
            road.setAverageSpeed(speed);

            roads.put(roadID, road);
            averageSpeed = speed;
            count = 1;
        } else {
            Road road = roads.get(roadID);

            int sum = 0;

            if (road.getRoadSpeedSize() < 2) {
                road.incrementCount();
                road.addRoadSpeed(speed);

                for (int it : road.getRoadSpeed()) {
                    sum += it;
                }

                averageSpeed = (int)((double)sum/(double)road.getRoadSpeedSize());
                road.setAverageSpeed(averageSpeed);
                count = road.getRoadSpeedSize();
            } else {
                double avgLast = roads.get(roadID).getAverageSpeed();
                double temp = 0;

                for (int it : road.getRoadSpeed()) {
                    sum += it;
                    temp += Math.pow((it-avgLast), 2);
                }

                int avgCurrent = (int) ((sum + speed)/((double)road.getRoadSpeedSize() + 1));
                temp = (temp + Math.pow((speed - avgLast), 2)) / (road.getRoadSpeedSize());
                double stdDev = Math.sqrt(temp);

                if (Math.abs(speed - avgCurrent) <= (2 * stdDev)) {
                    road.incrementCount();
                    road.addRoadSpeed(speed);
                    road.setAverageSpeed(avgCurrent);

                    averageSpeed = avgCurrent;
                    count = road.getRoadSpeedSize();
                }
            }
        }

        // emit unanchored tuple
        collector.emit(new Values(roadID, averageSpeed, count, timestamp));

        processed++;
        t_end = System.nanoTime();
    }

    @Override
    public void cleanup() {
        long t_elapsed = (t_end - t_start) / 1000000; // elapsed time in milliseconds

        System.out.println("[Calculator] execution time: " + t_elapsed +
                            " ms, processed: " + processed +
                            ", bandwidth: " + processed / (t_elapsed / 1000) +  // tuples per second
                            " tuples/s");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Field.ROAD_ID, Field.AVG_SPEED, Field.COUNT, Field.TIMESTAMP));
    }

}