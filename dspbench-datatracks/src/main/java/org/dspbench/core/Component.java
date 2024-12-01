package org.dspbench.core;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.dspbench.core.hook.Hook;
import org.dspbench.utils.Configuration;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
@Slf4j
public class Component implements Serializable {
    
    @Getter
    @Setter
    protected int id;
    @Getter
    @Setter
    protected String name;
    @Getter
    @Setter
    protected int parallelism;
    @Getter
    @Setter
    protected Map<String, Stream> outputStreams;
    protected Configuration config;
    
    protected List<Hook> highHooks;
    protected List<Hook> lowHooks;

    public Component() {
        outputStreams = new HashMap<>();
        
        highHooks = new ArrayList<>();
        lowHooks  = new ArrayList<>();
    }

    public void onCreate(int id, Configuration config) {
        this.id     = id;
        this.config = config;
        
        log.info("started component: class={}, name={}, id={}", this.getClass().getSimpleName(), name, id);
    }

    public void onDestroy() { }
    
    
    // Emit functions ----------------------------------------------------------
    protected void emit(Values values) {
        emit(Constants.DEFAULT_STREAM, null, values);
    }
    
    protected void emit(Tuple parent, Values values) {
        emit(Constants.DEFAULT_STREAM, parent, values);
    }
    
    protected void emit(String stream, Values values) {
        emit(stream, null, values);
    }
    
    protected void emit(String stream, Tuple parent, Values values) {
        hooksOnEmit(values);
        
        if (outputStreams.containsKey(stream)) {
            if (parent == null) {
                outputStreams.get(stream).put(this, values);
            } else {
                outputStreams.get(stream).put(this, parent, values);
            }
            
        } else {
            log.error("Stream {} not found at component {}, valid streams: {}.", stream, getFullName(), outputStreams.toString());
        }
    }
    
    
    // Output Streams ----------------------------------------------------------
    public void addOutputStream(String id, Stream stream) {
        if (!outputStreams.containsKey(id)) {
            outputStreams.put(id, stream);
        }
    }
    
    public void addOutputStream(Stream stream) {
        if (!outputStreams.containsKey(Constants.DEFAULT_STREAM)) {
            outputStreams.put(Constants.DEFAULT_STREAM, stream);
        }
    }


    public Stream getDefaultOutputStream() {
        if (outputStreams.containsKey(Constants.DEFAULT_STREAM))
            return outputStreams.get(Constants.DEFAULT_STREAM);
        else if (!outputStreams.isEmpty())
            return (Stream) outputStreams.values().toArray()[0];
        else
            return null;
    }


    // Hooks -------------------------------------------------------------------
    public void addHook(Hook hook) {
        if (hook.isHighPriority())
            highHooks.add(hook);
        else
            lowHooks.add(hook);
    }

    public void addHooks(List<Hook> hooks) {
        for (Hook hook : hooks)
            addHook(hook);
    }
    
    public void hooksOnEmit(Values values) {
        for (Hook hook : lowHooks)
            hook.onEmit(values);

        for (Hook hook : highHooks)
            hook.onEmit(values);
    }
    
    public void hooksBefore(Tuple tuple) {
        for (Hook hook : lowHooks)
            hook.beforeTuple(tuple);
        
        for (Hook hook : highHooks)
            hook.beforeTuple(tuple);
    }
    
    public void hooksAfter(Tuple tuple) {
        for (Hook hook : highHooks)
            hook.afterTuple(tuple);
        
        for (Hook hook : lowHooks)
            hook.afterTuple(tuple);
    }
    
    public void hooksOnReceive(Object value) {
        for (Hook hook : highHooks)
            hook.onSourceReceive(value);
        
        for (Hook hook : lowHooks)
            hook.onSourceReceive(value);
    }
    
    public void resetHooks() {
        highHooks = new ArrayList<>();
        lowHooks  = new ArrayList<>();
    }
    
    // Accessors ---------------------------------------------------------------
    public List<Hook> getHooks() {
        List<Hook> hooks = new ArrayList<>();
        hooks.addAll(highHooks);
        hooks.addAll(lowHooks);
        return hooks;
    }

    public void setHooks(List<Hook> hooks) {
        resetHooks();
        addHooks(hooks);
    }


    public String getFullName() {
        return String.format("%s-%d", name, id);
    }
    
    
    // Utils -------------------------------------------------------------------
    public Component newInstance() {
        try {
            return getClass().newInstance();
        } catch ( InstantiationException | IllegalAccessException ex) {
            log.error("Error while copying object", ex);
        }
        return null;
    }
    
    public Component copy() {
        Component newInstance = (Component) newInstance();
        
        if (newInstance != null) {
            newInstance.setName(name);
            newInstance.setParallelism(parallelism);
            newInstance.outputStreams = outputStreams;
            newInstance.highHooks = new ArrayList<>( highHooks );
            newInstance.lowHooks = new ArrayList<>( lowHooks );
        }
        
        return newInstance;
    }
}
