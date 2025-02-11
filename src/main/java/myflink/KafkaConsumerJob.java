package myflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class KafkaConsumerJob {

    public static void main(String[] args) throws Exception {

        // Create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set properties for Kafka consumer
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");

        // Create a Kafka consumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "commodity",
                new SimpleStringSchema(),
                properties
        );

        // Assign timestamps and watermarks
        kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps());

        // Add the Kafka consumer as a source to the execution environment
        env.addSource(kafkaConsumer)
                .name("Kafka Source")
                .print()
                .name("Print to Console");

        // Execute the Flink job
        env.execute("Kafka Consumer Job");
    }
}