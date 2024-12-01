package org.dspbench.topology;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.MetricRegistry;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dspbench.core.Task;
import org.dspbench.utils.Configuration;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
@Slf4j
public class TaskRunner {

    @Getter
    @Parameter(names = "-debug", description = "Debug mode")
    public boolean debug = false;

    @Parameter(names = "-task", description = "Full name of the class to be executed")
    public String task;

    @Parameter(names = "-name", description = "Name of the topology")
    public String name;

    @Parameter(names = "-config", description = "Configuration string")
    public String config;

    @Parameter(names = "-config-path", description = "Full path to the configuration file")
    public String configPath;

    @Getter
    private TopologyBuilder builder;
    @Getter
    private Topology topology;
    @Getter
    private Configuration configuration;
    private JCommander cli;
    private MetricRegistry metrics;

    @Getter
    private static Task task_;


    public TaskRunner( String[] args ) {
        builder = new TopologyBuilder();
        loadArgs( args );
    }


    private void loadArgs( String[] args ) {
        cli = new JCommander( this, args );
        log.info( "Loading configuration = {}", config == null ? configPath : config );
        configuration = createConfiguration();
        log.info( "Loaded configuration string into = {}", configuration );
    }


    protected Configuration createConfiguration() {
        if ( configPath != null ) {
            return Configuration.fromProperties( Objects.requireNonNull( Configuration.toProperties( configPath ) ) );
        } else if ( config != null ) {
            return Configuration.fromStr( config );
        } else {
            return new Configuration();
        }

    }


    public void start( ComponentFactory factory ) {
        start( name, task, factory );
    }


    public Task createTask( String taskName ) {
        if ( task_ != null ) {
            return task_;
        }
        try {
            Class<?> classObject = Class.forName( taskName );
            log.info( "Loaded task {} with configuration {}", taskName, configuration );

            task_ = (Task) classObject.getDeclaredConstructor().newInstance();

        } catch ( ClassNotFoundException ex ) {
            log.error( ex.getMessage(), ex );
            throw new RuntimeException( "Task not found", ex );
        } catch ( InstantiationException ex ) {
            log.error( ex.getMessage(), ex );
            throw new RuntimeException( "Unable to instantiate task", ex );
        } catch ( IllegalAccessException ex ) {
            log.error( ex.getMessage(), ex );
            throw new RuntimeException( "Unable to access task", ex );
        } catch ( InvocationTargetException | NoSuchMethodException e ) {
            throw new RuntimeException( e );
        }
        return task_;
    }


    public void start( String topologyName, String taskName, ComponentFactory factory ) {
        builder.setFactory( factory );
        builder.initTopology( name );
        log.info( "Initializing topology {}", name );

        createTask( taskName );
        topology = builder.build();
    }


}
