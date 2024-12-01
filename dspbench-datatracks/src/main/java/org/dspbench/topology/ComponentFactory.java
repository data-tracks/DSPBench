package org.dspbench.topology;

import org.dspbench.core.Operator;
import org.dspbench.core.Source;
import org.dspbench.core.Stream;
import org.dspbench.core.Schema;
import org.dspbench.utils.Configuration;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public interface ComponentFactory {
    Stream createStream( String name, Schema schema );
    IOperatorAdapter createOperatorAdapter( String name, Operator operator );
    ISourceAdapter createSourceAdapter( String name, Source source );
    Topology createTopology( String name );
    
    //public void setMetrics(MetricRegistry metrics);
    void setConfiguration( Configuration configuration );
}
