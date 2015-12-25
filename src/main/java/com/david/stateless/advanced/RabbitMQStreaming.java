package com.david.stateless.advanced;

import com.stratio.receiver.RabbitMQUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;

public class RabbitMQStreaming {
	public static void main(String... args) {

		// Creating a local Streaming Context with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("FlumeWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Create a DStream that will connect to rabbitmq
		Map<String, String> rabbitMQSettings = new HashMap();
		rabbitMQSettings.put("host", "localhost");
		rabbitMQSettings.put("exchangeName", "gateway");
		rabbitMQSettings.put("queueName", "request_q");
		rabbitMQSettings.put("vHost", "/");
		rabbitMQSettings.put("username", "test");
		rabbitMQSettings.put("password", "test");
		JavaReceiverInputDStream<String> lines = RabbitMQUtils.createJavaStream(jssc, rabbitMQSettings);


		// Split each line into words
		JavaDStream<String> words = lines.flatMap ( x -> Arrays.asList( x.split(" ")));

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair( x -> new Tuple2(x, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey( (i1, i2) -> i1 + i2 );

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();

		// Start the computation
		jssc.start();

		// Wait for the computation to terminate
		jssc.awaitTermination();
	}
}