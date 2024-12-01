package org.dspbench.topology;

import org.dspbench.core.Component;
import org.dspbench.core.hook.Hook;

import java.io.Serializable;

/**
 *
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 * @param <T>
 */
public interface IComponentAdapter<T extends Component> extends Serializable {
    void setComponent( T operator );
    T getComponent();
    void addComponentHook( Hook hook );
}
