package org.dbanelas;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.hipparchus.linear.RealMatrix;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LastKSegmentor extends KeyedProcessFunction<Integer, Batch, Tuple3<Long, Long, Integer>> {
    private static final double RV_EPSILON = 1e-12;

    // Max size of the open episode buffer;
    private final int K;
    private final double sigma;

    private transient ValueState<CircularBuffer<Batch>> openEpisodeBufferState;
    private transient Meter batchesProcessedMeter;

    private transient Gauge<Long> memoryUsed;

    public LastKSegmentor(int K, double sigma) {
        this.K = K;
        this.sigma = sigma;
    }

    @Override
    public void processElement(Batch right, KeyedProcessFunction<Integer, Batch, Tuple3<Long, Long, Integer>>.Context context, Collector<Tuple3<Long, Long, Integer>> collector) throws Exception {
        this.batchesProcessedMeter.markEvent();

        if (openEpisodeBufferState.value() == null) {
            openEpisodeBufferState.update(new CircularBuffer<>(K));
        }

        CircularBuffer<Batch> openEpisodeBuffer = openEpisodeBufferState.value();
        int bufferSize = openEpisodeBuffer.size();
        if (bufferSize < 1) {
            openEpisodeBuffer.add(right);
            openEpisodeBufferState.update(openEpisodeBuffer);
            return;
        }


        Batch left = openEpisodeBuffer.get(openEpisodeBuffer.size() - 1);
        if (isSegmentationWithBatch(left, right)) {
            emitEpisode(new ArrayList<>(openEpisodeBuffer.range(0, openEpisodeBuffer.size())), collector, context.getCurrentKey());
            openEpisodeBuffer.clear();
        } else {
            if (checkSegmentationInExponentialWindow(openEpisodeBuffer, right)) {
                emitEpisode(new ArrayList<>(openEpisodeBuffer.range(0, openEpisodeBuffer.size())), collector, context.getCurrentKey());
                openEpisodeBuffer.clear();
            }
        }

        openEpisodeBuffer.add(right);
        openEpisodeBufferState.update(openEpisodeBuffer);
    }

    private boolean checkSegmentationInExponentialWindow(CircularBuffer<Batch> openEpisodeBuffer, Batch right) {
        // Implement exponential window rv check with open episode batches
        int maxLeftLength = openEpisodeBuffer.size();
        int k = 2;
        while (Math.pow(2, k - 1) <= maxLeftLength) {
            int leftLength = (int) Math.pow(2, k - 1);
            int leftStart = maxLeftLength - leftLength;

            // Get the batches that belong to the exponential window
            List<Batch> exponentialWindowBatches = openEpisodeBuffer.range(leftStart, maxLeftLength);

            // Check for segmentation against the right batch and the
            // sum of exponential window batches
            if (isSegmentationWithBBt(sumBBt(exponentialWindowBatches), right.getBBt())) {
                return true;
            }
            k++;
        }
        return false;
    }

    private void emitEpisode(List<Batch> episodeBatches, Collector<Tuple3<Long, Long, Integer>> collector, Integer key) {
        Batch first = episodeBatches.get(0);
        Batch last = episodeBatches.get(episodeBatches.size() - 1);
        int tupleCount = episodeBatches.stream()
                        .map(Batch::getNumDataPoints)
                        .reduce(0, Integer::sum);

        collector.collect(new Tuple3<>(first.getMinTimestamp(), last.getMaxTimestamp(), tupleCount));
    }

    private boolean isSegmentationWithBatch(Batch left, Batch right) {
        double rv = SimilarityMeasureUtil.rv(left, right, RV_EPSILON);
        return rv < sigma;
    }

    private boolean isSegmentationWithBBt(RealMatrix left, RealMatrix right) {
        double rv = SimilarityMeasureUtil.rv(left, right, RV_EPSILON);
        return rv < sigma;
    }

    private RealMatrix sumBBt(List<Batch> batches) {
        RealMatrix sumBBt = batches.get(0).getBBt();
        for (int i = 1; i < batches.size(); i++) {
            sumBBt = sumBBt.add(batches.get(i).getBBt());
        }
        return sumBBt;
    }

    @Override
    public void open(Configuration configuration) {
        ValueStateDescriptor<CircularBuffer<Batch>> openEpisodeBufferStateDescriptor = new ValueStateDescriptor<>(
                "openEpisodeBufferState",
                TypeInformation.of(new TypeHint<>() {})
        );

        this.batchesProcessedMeter = getRuntimeContext()
                .getMetricGroup()
                .addGroup("throughput")
                .meter("batchesProcessedPerSecond", new MeterView(10));

        this.memoryUsed = getRuntimeContext()
                .getMetricGroup()
                .addGroup("memory")
                .gauge("memoryUsedForSegmentation", () -> {
                    CircularBuffer<Batch> buffer;
                    try {
                        buffer = openEpisodeBufferState.value();
                        if (buffer == null) return 0L;
                        return Mem.deepSize(buffer);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        this.openEpisodeBufferState = getRuntimeContext().getState(openEpisodeBufferStateDescriptor);
    }
}
