package org.dspbench.core;

import javax.xml.crypto.Data;
import org.dspbench.topology.Topology;
import org.dspbench.topology.TopologyBuilder;
import org.dspbench.topology.impl.DataTracksPlan;
import org.dspbench.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface Task {
    void setTopologyBuilder( TopologyBuilder builder );
    void setConfiguration( Configuration config );
    
    void initialize();
    Topology getTopology();
}
