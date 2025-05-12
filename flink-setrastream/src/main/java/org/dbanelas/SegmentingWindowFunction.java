package org.dbanelas;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.hipparchus.linear.RealMatrix;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SegmentingWindowFunction extends ProcessWindowFunction<Batch, Tuple3<Long, Long, Integer>, Integer, GlobalWindow> {
    // Constants used in the algorithm
    private final double RV_EPSILON = 1e-12;
    private final int NUM_POINTS_IN_EPISODE_PLACEHOLDER = 1;

    // Constructor variables
    private final int numBatchesInSegmentationWindow;
    private final double segmentationThreshold;

    // State variables
    private ValueState<Boolean> firstWindowState;
    private ListState<Batch> openEpisodeState;

    public SegmentingWindowFunction(int numBatchesInSegmentationWindow,
                                    double segmentationThreshold) {
        this.numBatchesInSegmentationWindow = numBatchesInSegmentationWindow;
        this.segmentationThreshold = segmentationThreshold;
    }

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> firstWindowStateDescriptor = new ValueStateDescriptor<>(
                "firstWindowState",
                TypeInformation.of(Boolean.class));

        ListStateDescriptor<Batch> openEpisodeBatchesStateDescriptor = new ListStateDescriptor<>(
                "openEpisodeBatches",
                TypeInformation.of(Batch.class));

        this.firstWindowState = getIterationRuntimeContext().getState(firstWindowStateDescriptor);
        this.openEpisodeState = getIterationRuntimeContext().getListState(openEpisodeBatchesStateDescriptor);
    }

    @Override
    public void process(Integer key,
                        ProcessWindowFunction<Batch, Tuple3<Long, Long, Integer>, Integer, GlobalWindow>.Context context,
                        Iterable<Batch> batches,
                        Collector<Tuple3<Long, Long, Integer>> collector) throws Exception {

        // Get the batches from the iterable
        List<Batch> windowBatchList = toImmutableList(batches);
        if (windowBatchList.size() != numBatchesInSegmentationWindow) {
            return; // Incomplete window, wait until a full window is received
        }

        final boolean isFirst = Boolean.TRUE.equals(firstWindowState.value());
        final Deque<Batch> openEpisodeBatches = loadOpenEpisodeBuffer();
        if (isFirst) {
            processFirstWindow(windowBatchList, openEpisodeBatches, collector);
            firstWindowState.update(false);
        } else {
            // Process later windows
            // Here, only the last batch needs to be compared against the exponential window
            // of open episodes
            processNonFirstWindow(openEpisodeBatches, windowBatchList.get(windowBatchList.size() - 1), collector);
        }

        openEpisodeState.update(new ArrayList<>(openEpisodeBatches));
    }

    private Deque<Batch> loadOpenEpisodeBuffer() throws Exception {
        Iterable<Batch> stateIterable = openEpisodeState.get();
        final Deque<Batch> buffer = new ArrayDeque<>();
        stateIterable.forEach(buffer::addLast);
        return buffer;

    }

    private void processNonFirstWindow(Deque<Batch> openEpisodeBatches,
                                       Batch right,
                                       Collector<Tuple3<Long, Long, Integer>> collector) {

        // Begin by checking the last batch of the open episodes against
        // the newly arrived right batch
        Batch left = openEpisodeBatches.getLast();
        if (isSegmentationWithBatch(left, right)) {
            emitEpisode(new ArrayList<>(openEpisodeBatches), collector);
            openEpisodeBatches.clear();
            openEpisodeBatches.addLast(right);
        } else {
            if (checkSegmentationInExponentialWindow(openEpisodeBatches, right)) {
                emitEpisode(new ArrayList<>(openEpisodeBatches), collector);
                openEpisodeBatches.clear();
                openEpisodeBatches.addLast(right);
            }
        }
    }


    private void processFirstWindow(List<Batch> windowBatches,
                                    Deque<Batch> openEpisodeBatches,
                                    Collector<Tuple3<Long, Long, Integer>> collector) {
        openEpisodeBatches.clear();
        openEpisodeBatches.addLast(windowBatches.get(0));
        int episodeStartIndex = 0;
        for (int i = 1; i < windowBatches.size(); i++) {
            Batch left = windowBatches.get(i - 1);
            Batch right = windowBatches.get(i);

            if (isSegmentationWithBatch(left, right)) {
                // If the RV coefficient is less than the threshold, we have segmentation and need to emit the current episode
                emitEpisode(windowBatches.subList(episodeStartIndex, i), collector);
                openEpisodeBatches.clear();
                episodeStartIndex = i;
            } else {

                if (checkSegmentationInExponentialWindow(openEpisodeBatches, right)) {
                    List<Batch> episodeBatches = windowBatches.subList(episodeStartIndex, i);
                    emitEpisode(episodeBatches, collector);
                    openEpisodeBatches.clear();
                    episodeStartIndex = i;
                }
                openEpisodeBatches.addLast(right);
            }
        }
    }

    private boolean checkSegmentationInExponentialWindow(Deque<Batch> openEpisodeBatches, Batch right) {
        // Implement exponential window rv check with open episode batches
        int maxLeftLength = openEpisodeBatches.size();
        int k = 2;
        while (Math.pow(2, k - 1) <= maxLeftLength) {
            int leftLength = (int) Math.pow(2, k - 1);
            int leftStart = maxLeftLength - leftLength;

            // Get the batches that belong to the exponential window
            List<Batch> exponentialWindowBatches = openEpisodeBatches.stream()
                    .skip(leftStart)
                    .collect(Collectors.toList());

            // Check for segmentation against the right batch and the
            // sum of exponential window batches
            if (isSegmentationWithBBt(sumBBt(exponentialWindowBatches), right.getBBt())) {
                return true;
            }
            k++;
        }
        return false;
    }


    private void emitEpisode(List<Batch> episodeBatches, Collector<Tuple3<Long, Long, Integer>> collector) {
        Batch first = episodeBatches.get(0);
        Batch last = episodeBatches.get(episodeBatches.size() - 1);
        collector.collect(new Tuple3<>(first.getMinTimestamp(), last.getMaxTimestamp(), NUM_POINTS_IN_EPISODE_PLACEHOLDER));
    }

    private boolean isSegmentationWithBatch(Batch left, Batch right) {
        double rv = SimilarityMeasureUtil.rv(left, right, RV_EPSILON);
        return rv < segmentationThreshold;
    }

    private boolean isSegmentationWithBBt(RealMatrix left, RealMatrix right) {
        double rv = SimilarityMeasureUtil.rv(left, right, RV_EPSILON);
        return rv < segmentationThreshold;
    }

    private List<Batch> toImmutableList(Iterable<Batch> batches) {
        return StreamSupport.stream(batches.spliterator(), false)
                .toList();
    }

    private RealMatrix sumBBt(List<Batch> batches) {
        RealMatrix sumBBt = batches.get(0).getBBt();
        for (int i = 1; i < batches.size(); i++) {
            sumBBt = sumBBt.add(batches.get(i).getBBt());
        }
        return sumBBt;
    }
}
