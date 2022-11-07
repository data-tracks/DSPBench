package org.dspbench.bolt;

import org.apache.commons.io.FileUtils;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.dspbench.constants.BaseConstants;
import org.dspbench.hooks.BoltMeterHook;
import org.dspbench.metrics.MetricsOutputCollector;
import org.dspbench.util.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.dspbench.util.config.Configuration.METRICS_ENABLED;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public abstract class AbstractBolt extends BaseRichBolt {
    protected String configPrefix = BaseConstants.BASE_PREFIX;

    protected OutputCollector collector;
    protected Configuration config;
    protected TopologyContext context;
    protected Map<String, Fields> fields;
    private BlockingQueue<String> queue;
    private File file;
    private static final Logger LOG = LoggerFactory.getLogger(AbstractBolt.class);
    private final Map<String, Long> throughput = new HashMap<>();

    public AbstractBolt() {
        fields = new HashMap<>();
    }

    public void setFields(Fields fields) {
        this.fields.put(BaseConstants.BaseStream.DEFAULT, fields);
    }

    public void setFields(String streamId, Fields fields) {
        this.fields.put(streamId, fields);
    }

    @Override
    public final void declareOutputFields(OutputFieldsDeclarer declarer) {
        if (fields.isEmpty()) {
            if (getDefaultFields() != null)
                fields.put(BaseConstants.BaseStream.DEFAULT, getDefaultFields());

            if (getDefaultStreamFields() != null)
                fields.putAll(getDefaultStreamFields());
        }

        for (Map.Entry<String, Fields> e : fields.entrySet()) {
            declarer.declareStream(e.getKey(), e.getValue());
        }
    }

    public Fields getDefaultFields() {
        return null;
    }

    public Map<String, Fields> getDefaultStreamFields() {
        return null;
    }

    public String getUnixTime() {
        long unixTime = 0;
        if (config.getString(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
            unixTime = Instant.now().getEpochSecond();
        } else {
            unixTime = Instant.now().toEpochMilli();
        }
        return unixTime + "";
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.config = Configuration.fromMap(stormConf);
        this.context = context;
        this.collector = collector;
        if (config.getBoolean(METRICS_ENABLED, false)) {
            File pathLa = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "latency").toFile();
            File pathTrh = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "throughput").toFile();

            pathLa.mkdirs();
            pathTrh.mkdirs();

            queue = new ArrayBlockingQueue<>(50);

            this.file = Paths.get(config.getString(Configuration.METRICS_OUTPUT), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
        }
        initialize();
    }

    public void initialize() {

    }

    public void calculateThroughput() {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            long unixTime = 0;
            if (config.getString(Configuration.METRICS_INTERVAL_UNIT).equals("seconds")) {
                unixTime = Instant.now().getEpochSecond();
            } else {
                unixTime = Instant.now().toEpochMilli();
            }

            Long ops = throughput.get(unixTime + "");
            if (ops == null) {
                for (Map.Entry<String, Long> entry : this.throughput.entrySet()) {
                    this.queue.add(entry.getKey() + "," + entry.getValue() + System.getProperty("line.separator"));
                }
                throughput.clear();
                if (queue.size() >= 10) {
                    SaveMetrics();
                }
            }

            ops = (ops == null) ? 1L : ++ops;

            throughput.put(unixTime + "", ops);
        }
    }

    public void calculateLatency(long UnixTimeInit) {
        if (config.getBoolean(Configuration.METRICS_ENABLED, false)) {
            try {
                FileWriter fw = new FileWriter(Paths.get(config.getString(Configuration.METRICS_OUTPUT), "latency", this.getClass().getSimpleName() + this.configPrefix + ".csv").toFile(), true);
                fw.write(Instant.now().toEpochMilli() - UnixTimeInit + System.getProperty("line.separator"));
                fw.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    LOG.error("Error while writing the file " + file, ex);
                }
            } catch (Exception e) {
                LOG.error("Error while creating the file " + e.getMessage());
            }
        }).start();
    }

    public void setConfigPrefix(String configPrefix) {
        this.configPrefix = configPrefix;
    }
}
