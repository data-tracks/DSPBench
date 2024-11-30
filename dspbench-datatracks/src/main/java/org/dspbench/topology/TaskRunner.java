package org.dspbench.topology;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.MetricRegistry;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import org.dspbench.core.Task;
import org.dspbench.utils.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class TaskRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TaskRunner.class);
    
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
    
    private TopologyBuilder builder;
    private Topology topology;
    private Configuration configuration;
    private JCommander cli;
    private MetricRegistry metrics;

    public TaskRunner(String[] args) {
        builder = new TopologyBuilder();
        loadArgs(args);
    }
    
    private void loadArgs(String[] args) {
        cli = new JCommander(this, args);
        LOG.info( "Loading configuration = {}", config == null ? configPath : config );
        configuration = createConfiguration();
        LOG.info( "Loaded configuration string into = {}", configuration );
    }
    
    protected Configuration createConfiguration() {
        if (configPath != null) {
            return Configuration.fromProperties( Objects.requireNonNull( Configuration.toProperties( configPath ) ) );
        }else if ( config != null ) {
            return Configuration.fromStr(config);
        }else {
            return new Configuration();
        }

    }
    
    public void start(ComponentFactory factory) {
        start(name, task, factory);
    }
    
    public void start(String topologyName, String taskName, ComponentFactory factory) {
        builder.setFactory(factory);
        builder.initTopology(name);
        LOG.info("Initializing topology {}", name);
        
        try {
            Class<?> classObject = Class.forName(taskName);
            LOG.info("Loaded task {} with configuration {}", taskName, configuration);

            Task task = (Task) classObject.getDeclaredConstructor().newInstance();
            task.setTopologyBuilder(builder);
            task.setConfiguration(configuration);
            task.initialize();
            
            topology = builder.build();
        } catch (ClassNotFoundException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Task not found", ex);
        } catch (InstantiationException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Unable to instantiate task", ex);
        } catch (IllegalAccessException ex) {
            LOG.error(ex.getMessage(), ex);
            throw new RuntimeException("Unable to access task", ex);
        } catch ( InvocationTargetException | NoSuchMethodException e ) {
            throw new RuntimeException( e );
        }
    }

    public TopologyBuilder getBuilder() {
        return builder;
    }
    
    public Topology getTopology() {
        return topology;
    }
    
    public Configuration getConfiguration() {
        return configuration;
    }
    
    public String getTopologyName() {
        return name;
    }
    
    public boolean isDebug() {
        return debug;
    }
}
