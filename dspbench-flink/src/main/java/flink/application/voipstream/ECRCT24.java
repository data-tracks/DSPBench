package flink.application.voipstream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import flink.constants.VoIPStreamConstants;
import flink.util.Metrics;
import flink.util.ODTDBloomFilter;

public class ECRCT24 extends Metrics implements FlatMapFunction<Tuple5<String, String, DateTime, Boolean, CallDetailRecord>, Tuple5<String, Long, Double, CallDetailRecord, String>>{
    private static final Logger LOG = LoggerFactory.getLogger(ECR.class);

    Configuration config;

    protected ODTDBloomFilter filter;
    protected String configPrefix;

    public ECRCT24(Configuration config, String configPrefix){
        super.initialize(config);
        this.config = config;
        this.configPrefix = configPrefix;

        int numElements       = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_NUM_ELEMENTS, configPrefix), 180000);
        int bucketsPerElement = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PEL, configPrefix), 10);
        int bucketsPerWord    = config.getInteger(String.format(VoIPStreamConstants.Conf.FILTER_BUCKETS_PWR, configPrefix), 16);
        double beta           = config.getDouble(String.format(VoIPStreamConstants.Conf.FILTER_BETA, configPrefix), 0.9672);
        
        filter = new ODTDBloomFilter(numElements, bucketsPerElement, beta, bucketsPerWord);
    }

    @Override
    public void flatMap(Tuple5<String, String, DateTime, Boolean, CallDetailRecord> value,
            Collector<Tuple5<String, Long, Double, CallDetailRecord, String>> out) throws Exception {
        super.initialize(config);
        super.incReceived();
        CallDetailRecord cdr = (CallDetailRecord) value.getField(4);
        
        if (cdr.isCallEstablished()) {
            String caller  = cdr.getCallingNumber();
            long timestamp = cdr.getAnswerTime().getMillis()/1000;

            // add numbers to filters
            filter.add(caller, 1, timestamp);
            double ecr = filter.estimateCount(caller, timestamp);
            super.incEmitted();
            out.collect(new Tuple5<String,Long,Double,CallDetailRecord, String>(caller, timestamp, ecr, cdr, configPrefix.toUpperCase()));
        }
    }
}
