package flink.application.smartgrid;

import flink.application.AbstractApplication;
import flink.constants.SmartGridConstants;
import flink.parsers.SmartPlugParser;
import flink.source.InfSourceFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmartGrid extends AbstractApplication {

    private static final Logger LOG = LoggerFactory.getLogger(SmartGrid.class);

    private int sourceThreads;
    private int parserThreads;
    private int slidingWindowThreads;
    private int globalMedianThreads;
    private int plugMedianThreads;
    private int outlierDetectorThreads;
    private int houseLoadThreads;
    private int plugLoadThreads;
    private int houseLoadFrequency;
    private int plugLoadFrequency;

    public SmartGrid(String appName, Configuration config) {
        super(appName, config);
    }

    @Override
    public void initialize() {
        sourceThreads = config.getInteger(SmartGridConstants.Conf.SOURCE_THREADS, 1);
        parserThreads = config.getInteger(SmartGridConstants.Conf.PARSER_THREADS, 1);
        slidingWindowThreads = config.getInteger(SmartGridConstants.Conf.SLIDING_WINDOW_THREADS, 1);
        globalMedianThreads = config.getInteger(SmartGridConstants.Conf.GLOBAL_MEDIAN_THREADS, 1);
        plugMedianThreads = config.getInteger(SmartGridConstants.Conf.PLUG_MEDIAN_THREADS, 1);
        outlierDetectorThreads = config.getInteger(SmartGridConstants.Conf.OUTLIER_DETECTOR_THREADS, 1);
        houseLoadThreads = config.getInteger(SmartGridConstants.Conf.HOUSE_LOAD_THREADS, 1);
        plugLoadThreads = config.getInteger(SmartGridConstants.Conf.PLUG_LOAD_THREADS, 1);
        houseLoadFrequency = config.getInteger(SmartGridConstants.Conf.HOUSE_LOAD_FREQUENCY, 15);
        plugLoadFrequency = config.getInteger(SmartGridConstants.Conf.PLUG_LOAD_FREQUENCY, 15);
    }

    @Override
    public StreamExecutionEnvironment buildApplication() {

        env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Spout
        // DataStream<String> data = createSource();

        InfSourceFunction source = new InfSourceFunction(config, getConfigPrefix());
        DataStream<String> data = env.addSource(source).setParallelism(sourceThreads);

        // Parser
        DataStream<Tuple8<String, Long, Double, Integer, String, String, String, String>> dataParse = data
                .map(new SmartPlugParser(config)).setParallelism(parserThreads);

        // Process
        DataStream<Tuple7<Long, String, String, String, Double, Integer, String>> slideWindow = dataParse
                .filter(value -> (value != null)).flatMap(new SlideWindow(config)).setParallelism(slidingWindowThreads);

        DataStream<Tuple5<String, String, Long, Double, String>> globalMedCalc = slideWindow
                .flatMap(new GlobalMedianCalc(config)).setParallelism(globalMedianThreads);

        DataStream<Tuple5<String, String, Long, Double, String>> plugMedCalc = slideWindow.keyBy(
                new KeySelector<Tuple7<Long, String, String, String, Double, Integer, String>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(
                            Tuple7<Long, String, String, String, Double, Integer, String> value) throws Exception {
                        return Tuple3.of(value.f1, value.f2, value.f3);
                    }
                }).flatMap(new PlugMedianCalc(config)).setParallelism(plugMedianThreads);

        DataStream<Tuple5<Long, Long, String, Double, String>> outlierDetect = globalMedCalc
                .connect(plugMedCalc.keyBy(value -> value.f1)).flatMap(new OutlierDetect(config))
                .setParallelism(outlierDetectorThreads);
        // DataStream<Tuple5<Long, Long, String, Double, String>> outlierDetectGlobal =
        // globalMedCalc.flatMap(new
        // OutlierDetect(config)).setParallelism(outlierDetectorThreads);

        DataStream<Tuple4<Long, String, Double, String>> houseLoadPredictor = dataParse.keyBy(value -> value.f6)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(houseLoadFrequency)))
                .apply(new HouseLoadPredict(config)).setParallelism(houseLoadThreads);

        DataStream<Tuple6<Long, String, String, String, Double, String>> plugLoadPredictor = dataParse
                .keyBy(value -> value.f6).window(TumblingProcessingTimeWindows.of(Time.seconds(plugLoadFrequency)))
                .apply(new PlugLoadPredict(config)).setParallelism(plugLoadThreads);

        // Sink
        createSinkSGOutlier(outlierDetect, "outlier");

        createSinkSGHouse(houseLoadPredictor, "predictionHouse");
        createSinkSGPlug(plugLoadPredictor, "predictionPlug");

        return env;
    }

    @Override
    public String getConfigPrefix() {
        return SmartGridConstants.PREFIX;
    }

    @Override
    public Logger getLogger() {
        return LOG;
    }
}
