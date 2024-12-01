package org.dspbench.topology.impl;

import java.util.List;
import lombok.Value;
import org.dspbench.topology.ISourceAdapter;
import org.dspbench.utils.Configuration;

@Value
public class DataTracksPlan {
    String name;
    String plan;
    List<LocalSourceAdapter> sources;


}
