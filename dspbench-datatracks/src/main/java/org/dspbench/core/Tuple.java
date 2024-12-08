package org.dspbench.core;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;

/**
 * 
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class Tuple implements Serializable {
    private static final long serialVersionUID = -5139144941369700193L;
    
    @Setter
    @Getter
    protected long id;
    @Getter
    protected long lineageBirth;
    @Getter
    protected long createdAt;
    @Setter
    @Getter
    protected String componentName;
    @Setter
    @Getter
    protected int componentId;
    @Setter
    @Getter
    protected String streamId;
    
    @Setter
    @Getter
    protected transient Object tempValue;
    
    protected Map<String, Serializable> map;

    public Tuple() {
        this(null);
    }
    
    public Tuple(Tuple parent) {
        map          = new HashMap<>();
        createdAt    = System.currentTimeMillis();
        lineageBirth = (parent != null) ? parent.lineageBirth : createdAt;
    }

    public Tuple(long id, int componentId, String componentName, String streamId,
            long lineageBirth, long createdAt, Map<String, Serializable> map) {
        this.id= id;
        this.lineageBirth = lineageBirth;
        this.createdAt = createdAt;
        this.componentName = componentName;
        this.componentId = componentId;
        this.streamId = streamId;
        this.map = map;
    }
    
    public void put(String key, Serializable value) {
        map.put(key, value);
    }
    
    public Serializable get(String key) {
        return map.get(key);
    }
    
    public String getString(String key) {
        return (String) map.get(key);
    }
    
    public Long getLong(String key) {
        return (Long) map.get(key);
    }
    
    public Integer getInt(String key) {
        return (Integer) map.get(key);
    }
    
    public Float getFloat(String key) {
        return (Float) map.get(key);
    }
    
    public Double getDouble(String key) {
        return (Double) map.get(key);
    }
    
    public Boolean getBoolean(String key) {
        return (Boolean) map.get(key);
    }
    
    public Object getValue(String key) {
        return map.get(key);
    }
    
    public List<Serializable> getValueList() {
        return new ArrayList<Serializable>(map.values());
    }

    public boolean contains(String key) {
        return map.containsKey(key);
    }

    public Values getValues() {
        Values values = new Values();
        values.addAll(map.values());
        return values;
    }
    
    public Map<String, Serializable> getEntries() {
        return map;
    }


    @Override
    public String toString() {
        return "Tuple{" + "id=" + id + ", lineageBirth=" + lineageBirth 
                + ", createdAt=" + createdAt + ", componentName=" + componentName 
                + ", componentId=" + componentId + ", streamId=" + streamId 
                + ", map=" + map + '}';
    }

    public long sizeOf() {
        long size = 0;
        
        for (Object o : map.values()) {
            size += RamUsageEstimator.sizeOf(o);
        }
        
        return size;
    }
    
    public static final class TupleSerializer extends Serializer<Tuple> {

        @Override
        public void write(Kryo kryo, Output output, Tuple t) {
            output.writeLong(t.id);
            output.writeInt(t.componentId);
            output.writeString(t.componentName);
            output.writeString(t.streamId);
            output.writeLong(t.lineageBirth);
            output.writeLong(t.createdAt);
            kryo.writeObject(output, t.map);
        }

        @Override
        public Tuple read(Kryo kryo, Input input, Class<? extends Tuple> type) {
            return new Tuple(input.readLong(), input.readInt(), input.readString(),
                    input.readString(), input.readLong(), input.readLong(),
                    kryo.readObject(input, HashMap.class));
        }
        
    }
}
