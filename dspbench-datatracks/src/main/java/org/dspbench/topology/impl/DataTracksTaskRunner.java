package org.dspbench.topology.impl;

import com.codahale.metrics.MetricRegistry;
import org.dspbench.metrics.MetricsFactory;
import org.dspbench.topology.TaskRunner;
import org.dspbench.utils.Configuration;

public class DataTracksTaskRunner extends TaskRunner {

    public DataTracksTaskRunner( String[] args ) {
        super( args );
        createTask( task );
    }


    public static void main(String[] args) {
        DataTracksTaskRunner taskRunner = new DataTracksTaskRunner(args);
        taskRunner.createConfiguration();
        MetricRegistry metrics = MetricsFactory.createRegistry( taskRunner.getConfiguration() );

        DataTracksEngine engine = DataTracksEngine.getEngine();
        engine.setRegistry( metrics );
        engine.setPlan(taskRunner.getPlan(taskRunner.getConfiguration()));
        engine.run();
    }

    private DataTracksPlan getPlan( Configuration config ) {
        getTask_().setConfiguration( config );
        return ((DataTracksTask) getTask_()).getPlan();
    }


}
