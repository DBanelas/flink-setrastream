package org.dbanelas;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class BatchingWindowFunction extends ProcessWindowFunction<DataPoint, Batch, Integer, TimeWindow> {

    private transient Meter tuplesPerSecond;

    @Override
    public void open(Configuration parameters) throws Exception {
        this.tuplesPerSecond = getRuntimeContext().getMetricGroup()
                .addGroup("throughput")
                .meter("tuplesPerSecond", new MeterView(10));
    }

    @Override
    public void process(Integer key,
                        ProcessWindowFunction<DataPoint, Batch, Integer, TimeWindow>.Context context,
                        Iterable<DataPoint> dataPointsIterable,
                        Collector<Batch> collector) throws Exception {
        List<DataPoint> dataPoints = new ArrayList<>();
        // Timestamp of the batch is the maximum timestamp of the DataPoints inside it
        long minTimestamp = Long.MAX_VALUE;
        long maxTimestamp = Long.MIN_VALUE;
        for (DataPoint dataPoint : dataPointsIterable) {
            dataPoints.add(dataPoint);
            this.tuplesPerSecond.markEvent();
            minTimestamp = Math.min(minTimestamp, dataPoint.getTimestamp());
            maxTimestamp = Math.max(maxTimestamp, dataPoint.getTimestamp());
        }
        // ID of the batch corresponds to the ID of the DataPoints inside it
        int batchId = dataPoints.get(0).getId();

        collector.collect(new Batch(dataPoints, batchId, minTimestamp, maxTimestamp));
    }
}
