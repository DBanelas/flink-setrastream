/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dbanelas;

import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DataStreamJob {

    private static final Logger LOG = LoggerFactory.getLogger(DataStreamJob.class);

    public static void main(String[] args) throws Exception {

        Namespace ns = ArgumentParserUtil.parseArguments(args);

        String kafkaHost = ns.getString("kafkaHost");
        int kafkaPort = ns.getInt("kafkaPort");
        String inputTopic = ns.getString("inputTopic");
        String outputTopic = ns.getString("outputTopic");
        List<String> featureColumns = ns.getList("featureColumns");
        String idColumn = ns.getString("id");
        String timestampColumn = ns.getString("timestamp");
        int batchWindow = ns.getInt("batchWindow");
        int segmentationWindow = ns.getInt("segmentationWindow");
        int parallelism = ns.getInt("parallelism");
        String bootstrapServer = kafkaHost + ":" + kafkaPort;
        int numBatchesInSegmentationWindow = segmentationWindow / batchWindow;

        System.out.println("Running job with Kafka host:port " + bootstrapServer + " and input topic " + inputTopic);
        System.out.println("Feature columns: " + featureColumns);
        System.out.println("ID column: " + idColumn);
        System.out.println("Timestamp column: " + timestampColumn);
        System.out.println("Batch window: " + batchWindow);
        System.out.println("Segmentation window: " + segmentationWindow);
        System.out.println("Number of tuples per batch: " + batchWindow / 100);
        System.out.println("Number of batches in segmentation window: " + numBatchesInSegmentationWindow);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        WatermarkStrategy<DataPoint> watermarkStrategy =
                WatermarkStrategy.<DataPoint>forMonotonousTimestamps()
                        .withTimestampAssigner((dataPoint, timestamp) -> dataPoint.getTimestamp());

        KafkaSource<DataPoint> source = KafkaSource.<DataPoint>builder()
                .setBootstrapServers(bootstrapServer)
                .setTopics(inputTopic)
                .setGroupId("robot-job")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new DataPointDeserialization(featureColumns, idColumn, timestampColumn))
                .build();

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServer)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build())
                .build();

        DataStream<DataPoint> dataStream = env
                .fromSource(source, watermarkStrategy, "Robot Data Source");
        // Create batches of tuples for
        DataStream<Batch> batchStream = dataStream
                .keyBy(DataPoint::getId)
                .window(TumblingEventTimeWindows.of(Duration.ofMillis(batchWindow)))
                .process(new BatchingWindowFunction())
                .name("WindowedBatchCreation");

        DataStreamSink<String> segmentStream = batchStream
                .keyBy(Batch::getId)
                .process(new LastKSegmentor(numBatchesInSegmentationWindow, 0.7))
                .name("TrajectorySegmentation")
                .map(tuple -> tuple.f0 + ", " + tuple.f1)
                .sinkTo(sink);

//		DataStreamSink<String> segmentStream = batchStream
//				.keyBy(Batch::getId)
//				.countWindow(numBatchesInSegmentationWindow, 1)
//				.process(new SegmentingWindowFunction(numBatchesInSegmentationWindow, 0.7))
//                .name("TrajectorySegmentation")
//                .map(tuple -> tuple.f0 + ", " + tuple.f1)
//                .sinkTo(sink);

        // Execute program, beginning computation.
        env.execute("Semantic Trajectory Segmentation Job");
    }
}

