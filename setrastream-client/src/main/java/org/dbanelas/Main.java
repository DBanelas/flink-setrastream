package org.dbanelas;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.Properties;

public class Main {
    private static final String TOPIC = "robot_data";
    private static final String BOOTSTRAP_SERVER = "localhost:9092";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String ROBOT_DATA_FILE_PATH = "/Users/dbanelas/Developer/flink-setrastream/data/realistic_robot_trajectories_1000_robots_sorted_dpx_dpy.csv";
//    private static final String ROBOT_DATA_FILE_PATH = "/Users/dbanelas/Developer/flink-setrastream/data/smart_factory_with_collisions_100_robots_sorted_dpx_dpy.csv";

    public static void main(String[] args) throws Exception {
        Path csv = Path.of(args.length > 0 ? args[0] : ROBOT_DATA_FILE_PATH);
        try (KafkaProducer<String, String> producer = getKafkaProducer(BOOTSTRAP_SERVER);
        BufferedReader br = new BufferedReader(new FileReader(csv.toFile()))) {
            System.out.println("Sending data to Kafka topic: " + TOPIC);
            String header = br.readLine();
            String line;
            long count = 0;
            long totalSizeSent = 0;
            while ((line = br.readLine()) != null) {
                RobotData row = parse(line);
                String key = row.robotID() + "-" + row.currentTimeStep();
                String json   = MAPPER.writeValueAsString(row);

                long keyBytes = key.getBytes().length;
                long jsonBytes = json.getBytes().length;
                totalSizeSent += keyBytes + jsonBytes;

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, json);
                producer.send(record);
                count++;
            }
            producer.flush();
            double mb = totalSizeSent / 1024d / 1024d;
            System.out.printf("Sent %,d records (%.2f MB)%n", count, mb);
        }
    }

    /** Converts a CSV row to the POJO */
    private static RobotData parse(String csvLine) {
        String[] f = csvLine.split(",", -1);   // keep empty strings intact

        return new RobotData(
                Integer.parseInt(f[0]),
                Double.parseDouble(f[1]),
                Integer.parseInt(f[2]),
                Double.parseDouble(f[3]),
                Double.parseDouble(f[4]),
                Double.parseDouble(f[5]),
                Double.parseDouble(f[6]),
                Double.parseDouble(f[7]),
                f[8],
                Boolean.parseBoolean(f[9]),
                Boolean.parseBoolean(f[10]),
                Boolean.parseBoolean(f[11]),
                Boolean.parseBoolean(f[12]),
                Boolean.parseBoolean(f[13]),
                Double.parseDouble(f[14]), // dpx
                Double.parseDouble(f[15]) // dpy
        );
    }

    /**
     * Method to create and return a KafkaProducer instance
     * @param bootstrapServer The hostName:port string for the Kafka broker
     * @return The created KafkaProducer instance
     */
    public static KafkaProducer<String, String> getKafkaProducer(String bootstrapServer){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }
}