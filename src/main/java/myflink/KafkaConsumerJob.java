package myflink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.bson.Document;
import redis.clients.jedis.Jedis;

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
        DataStream<String> stream = env.addSource(kafkaConsumer)
                .name("Kafka Source");

        // Modify the data
        DataStream<String> modifiedStream = stream.map(value -> {
            // Replace single quotes with double quotes
            String jsonString = value.replace("'", "\"");
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode node = (ObjectNode) mapper.readTree(jsonString);
            node.put("name", "Modified: " + node.get("name").asText());

            // Store data in Redis cache
            try (Jedis jedis = new Jedis("localhost", 6379)) {
                String cacheKey = "commodity:" + node.get("id").asInt();
                jedis.set(cacheKey, node.toString());
                jedis.expire(cacheKey, 3600); // Set expiration time to 1 hour
            } catch (Exception e) {
                e.printStackTrace();
            }

            // Store data in MongoDB
            try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {
                MongoDatabase database = mongoClient.getDatabase("test");
                MongoCollection<Document> collection = database.getCollection("commodities");

                Document doc = new Document("id", node.get("id").asInt())
                        .append("name", node.get("name").asText())
                        .append("timestamp", node.get("timestamp").asLong());

                collection.insertOne(doc);
            } catch (Exception e) {
                e.printStackTrace();
            }

            return node.toString();
        });

        // Print the modified data
        modifiedStream.print().name("Print to Console");

        // Execute the Flink job
        env.execute("Kafka Consumer Job");
    }
}