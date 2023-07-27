package flink.sink;

import flink.constants.BaseConstants;
import flink.util.Metrics;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public abstract class BaseSink extends Metrics {
    private static final Logger LOG = LoggerFactory.getLogger(BaseSink.class);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    Configuration config;

    public void initialize(Configuration config) {
        super.initialize(config);
        this.config = config;
    }

    protected String getConfigKey(String template) {
        return String.format(template, configPrefix);
    }

    protected abstract Logger getLogger();

    public void sinkStreamWC(DataStream<Tuple2<String, Integer>> dt) {
    }

    public void sinkStreamTM(DataStream<Tuple4<Date, Integer, Integer, Integer>> dt) {
    }

    public void sinkStreamSD(DataStream<Tuple4<String, Double, Double, String>> dt) {
    }

    public void sinkStreamSGOutlier(DataStream<Tuple5<Long, Long, String, Double, String>> dt, String sinkName) {
    }

    public void sinkStreamSGHouse(DataStream<Tuple4<Long, String, Double, String>> dt, String sinkName) {
    }

    public void sinkStreamSGPlug(DataStream<Tuple6<Long, String, String, String, Double, String>> dt, String sinkName) {
    }

    public void sinkStreamSA(DataStream<Tuple5<String, String, Date, String, Double>> dt) {
    }

    public void sinkStreamMO(DataStream<Tuple6<String, Double, Long, Boolean, Object, String>> dt) {
    }

    public void sinkStreamFD(DataStream<Tuple4<String, Double, String, String>> dt) {
    }

    public void createSinkLPVol(DataStream<Tuple2<Long, Long>> dt, String sinkName) {
    }

    public void createSinkLPStatus(DataStream<Tuple2<Integer, Integer>> dt, String sinkName) {
    }

    public void createSinkLPGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt, String sinkName) {
    }

    public void createSinkCAStatus(DataStream<Tuple2<Integer, Integer>> dt, String sinkName) {
    }

    public void createSinkCAGeo(DataStream<Tuple4<String, Integer, String, Integer>> dt, String sinkName) {
    }
}
