package org.dspbench.topology.impl;

import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.dspbench.topology.ISourceAdapter;

@Slf4j
public class DataTracksEngine {

    @Getter
    public final static DataTracksEngine engine = new DataTracksEngine();

    @Setter
    private DataTracksPlan plan;

    @Setter
    private MetricRegistry registry;

    private final List<ExecutorService> threadPool = new ArrayList<>();



    public void run() {
        log.info("Running plan {} in DataTracks", plan.getName());

        // operate plan instances


        // start sources
        for ( LocalSourceAdapter adapter : plan.getSources()) {
            log.info("Initializing {} source with {} threads", adapter.getComponent().getName(), adapter.getComponent().getParallelism());
            adapter.setupInstances();
            ExecutorService executor = Executors.newFixedThreadPool(adapter.getComponent().getParallelism());
            threadPool.add(executor);

            for (LocalSourceInstance srcInstance : adapter.getInstances()) {
                executor.execute(srcInstance);
            }
        }

        log.info("Running...");
    }

}
