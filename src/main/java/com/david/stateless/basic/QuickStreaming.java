package com.david.stateless.basic;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;

public class QuickStreaming {
	public static void main(String... args) {

		// Creating a local Streaming Context with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

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