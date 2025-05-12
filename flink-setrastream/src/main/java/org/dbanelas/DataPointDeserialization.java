package org.dbanelas;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.*;

public class DataPointDeserialization implements KafkaRecordDeserializationSchema<DataPoint> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final List<String> featureColumns;
    private final String idColumn;
    private final String timestampColumn;

    public DataPointDeserialization(List<String> featureColumns, String idColumn, String timestampColumn) {
        this.featureColumns = featureColumns;
        this.idColumn = idColumn;
        this.timestampColumn = timestampColumn;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<DataPoint> collector) throws IOException {
        JsonNode root = MAPPER.readTree(consumerRecord.value());

        int id = root.get(this.idColumn).asInt();
        long timestamp = (long) (root.get(this.timestampColumn).asDouble() * 1000); // ms

        List<Double> features = new ArrayList<>();
        for (String f : featureColumns) {
            JsonNode n = root.get(f);
            if (n != null && !n.isNull()) {
                features.add(n.asDouble());
            }
        }
        collector.collect(new DataPoint(id, timestamp, features));
    }

    @Override
    public TypeInformation<DataPoint> getProducedType() {
        return TypeInformation.of(DataPoint.class);
    }
}
