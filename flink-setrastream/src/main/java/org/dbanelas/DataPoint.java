package org.dbanelas;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

/**
 * Generic container for a robot measurement.
 * `id` – a stable identifier
 * `ts` – logical timestamp
 * `features`-– only the fields the user asked for, name → value.
 */
public class DataPoint implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int id;
    private final long timestamp;
    private final List<Double> features;

    public DataPoint(int id, long timestamp, List<Double> features) {
        this.id = id;
        this.timestamp = timestamp;
        this.features = features;
    }

    // Getters
    public int getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public List<Double> getFeatures() {
        return features;
    }


    // Implement to string method
    @Override
    public String toString() {
        return "DataPoint{" +
                "id=" + id +
                ", timestamp=" + timestamp +
                ", features=" + features +
                '}';
    }
}
