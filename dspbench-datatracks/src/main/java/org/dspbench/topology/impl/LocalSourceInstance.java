package org.dspbench.topology.impl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.dspbench.core.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@Getter
public class LocalSourceInstance implements Runnable {
    private final Source source;
    private final int index;

    public LocalSourceInstance(Source source, int index) {
        this.source = source;
        this.index = index;
    }


    public void run() {
        while (source.hasNext()) {
            source.hooksBefore(null);
            source.nextTuple();
            source.hooksAfter(null);
        }
        
        log.info("Source {} finished", source.getDefaultOutputStream());
    }
}