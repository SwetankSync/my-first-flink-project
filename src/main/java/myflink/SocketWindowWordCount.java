package myflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        // Create the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the time characteristic to EventTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // Obtain the input data by connecting to the socket. Here you want to connect to the local 9000 port. If 9000 port is not available, you'll need to change the port.
        DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

        // Assign timestamps and watermarks
        DataStream<String> withTimestampsAndWatermarks = text
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis())
                );

        // Parse the data, group by word, and perform the window and aggregation operations.
        DataStream<Tuple2<String, Integer>> windowCounts = withTimestampsAndWatermarks
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // Print the results to the console. Note that here single-threaded printed is used, rather than multi-threading.
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }
}