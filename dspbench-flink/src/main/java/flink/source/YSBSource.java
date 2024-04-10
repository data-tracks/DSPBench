package flink.source;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import flink.constants.BaseConstants;
import flink.util.Configurations;
import flink.util.MetricsFactory;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;

import org.apache.flink.configuration.Configuration;

public class YSBSource extends RichParallelSourceFunction<YSB_Event>{
    private volatile boolean isRunning = true;
    Configuration config;

    private List<CampaignAd> campaigns;
    private int n_campaigns;
    private final static List<String> AD_TYPES = Arrays.asList("banner", "modal", "sponsored-search", "mail", "mobile");
    private final static List<String> EVENT_TYPES = Arrays.asList("view", "click", "purchase");
    private int adTypeLength;
    private int eventTypeLength;
    private int campaignLength;
    private int i;
    private int j;
    private int k;
    private long generated;
    private long runTimeSec;
    private String uuid;

    MetricsSource metric = new MetricsSource();

    public YSBSource(Configuration config, List<CampaignAd> campaigns, int n_campaigns, long runTime) {
        this.campaigns = campaigns;
        this.n_campaigns = n_campaigns;
        this.adTypeLength = 5;
        this.eventTypeLength = 3;
        this.campaignLength = campaigns.size();
        this.i=0;
        this.j=0;
        this.k=0;
        this.generated = 0;
        this.runTimeSec = runTime;

        metric.initialize(config);
    }

    @Override
    public void run(SourceContext<YSB_Event> ctx) throws Exception {
        long epoch = System.nanoTime();
        // generation loop
        while ((System.nanoTime() - epoch < runTimeSec * 1e9) && isRunning) {
            i += 1;
            j += 1;
            k += 1;
            if (i >= campaignLength) {
                i = 0;
            }
            if (j >= adTypeLength) {
                j = 0;
            }
            if (k >= eventTypeLength) {
                k = 0;
            }
            String ad_id = (campaigns.get(i)).ad_id; // ad id for the current event index
            String ad_type = AD_TYPES.get(j); // current adtype for event index
            String event_type = EVENT_TYPES.get(k); // current event type for event index
            String ip = "255.255.255.255";
            long ts = System.nanoTime();
            long timestamp = System.currentTimeMillis();
            ctx.collectWithTimestamp(new YSB_Event(uuid, uuid, ad_id, ad_type, event_type, ts, ip), timestamp);
            if (generated % 1000 == 0)
                ctx.emitWatermark(new Watermark(timestamp));
            generated++;
            // set the starting time
            if (generated == 1) {
                epoch = System.nanoTime();
            }
            metric.incEmitted();
        }
        // terminate the generation
        isRunning = false;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

class MetricsSource implements Serializable {
    Configuration config;
    private final BlockingQueue<String> queue = new ArrayBlockingQueue<>(150);
    protected String configPrefix = BaseConstants.BASE_PREFIX;
    private File file;

    private static MetricRegistry metrics;
    private Counter tuplesReceived;
    private Counter tuplesEmitted;

    public void initialize(Configuration config) {
        this.config = config;
        getMetrics();
        File pathTrh = Paths.get(config.getString(Configurations.METRICS_OUTPUT,"/home/IDK")).toFile();

        pathTrh.mkdirs();

        this.file = Paths.get(config.getString(Configurations.METRICS_OUTPUT, "/home/IDK"), "throughput", this.getClass().getSimpleName() + "_" + this.configPrefix + ".csv").toFile();
    }

    public void SaveMetrics() {
        new Thread(() -> {
            try {
                try (Writer writer = new FileWriter(this.file, true)) {
                    writer.append(this.queue.take());
                } catch (IOException ex) {
                    System.out.println("Error while writing the file " + file + " - " + ex);
                }
            } catch (Exception e) {
                System.out.println("Error while creating the file " + e.getMessage());
            }
        }).start();
    }

    protected MetricRegistry getMetrics() {
        if (metrics == null) {
            metrics = MetricsFactory.createRegistry(config);
        }
        return metrics;
    }

    protected Counter getTuplesReceived() {
        if (tuplesReceived == null) {
            tuplesReceived = getMetrics().counter("YSBSource-received");
        }
        return tuplesReceived;
    }

    protected Counter getTuplesEmitted() {
        if (tuplesEmitted == null) {
            tuplesEmitted = getMetrics().counter("YSBSource-emitted");
        }
        return tuplesEmitted;
    }

    protected void incReceived() {
        getTuplesReceived().inc();
    }

    protected void incReceived(long n) {
        getTuplesReceived().inc(n);
    }

    protected void incEmitted() {
        getTuplesEmitted().inc();
    }

    protected void incEmitted(long n) {
        getTuplesEmitted().inc(n);
    }

    protected void incBoth() {
        getTuplesReceived().inc();
        getTuplesEmitted().inc();
    }
}
