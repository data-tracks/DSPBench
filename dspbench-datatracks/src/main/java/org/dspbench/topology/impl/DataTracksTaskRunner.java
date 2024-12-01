package org.dspbench.topology.impl;

import com.codahale.metrics.MetricRegistry;
import org.dspbench.metrics.MetricsFactory;
import org.dspbench.topology.TaskRunner;

public class DataTracksTaskRunner extends TaskRunner {

    public DataTracksTaskRunner( String[] args ) {
        super( args );
        createTask( task );
    }


    public static void main(String[] args) {
        DataTracksTaskRunner taskRunner = new DataTracksTaskRunner(args);
        MetricRegistry metrics = MetricsFactory.createRegistry( taskRunner.getConfiguration() );

        DataTracksEngine engine = DataTracksEngine.getEngine();
        engine.setRegistry( metrics );
        engine.setPlan(taskRunner.getPlan());
        engine.run();
    }

    private DataTracksPlan getPlan() {
        return ((DataTracksTask) getTask_()).getPlan();
    }


}
