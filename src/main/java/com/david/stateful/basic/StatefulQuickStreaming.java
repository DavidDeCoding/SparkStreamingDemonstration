package com.david.stateful.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.*;
import com.google.common.base.Optional;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StatefulQuickStreaming {
	public static void main(String... args) {

		// Creating a local Streaming Context with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("StatefulQuickWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		// A checkpoint is configured, or else mapWithState will complaint.
		jssc.checkpoint("./checkpoints"); // If spark crashes, it stores its previous state in this directory, so that it can continue when it comes back online.

		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

		// Split each line into words
		JavaDStream<String> words = lines.flatMap ( x -> Arrays.asList( x.split(" ")));

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair( x -> new Tuple2(x, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey( (nums, current) -> {
				Integer sum = current.or(0);
				for (Integer i : nums) {
					sum += i;
				}
				return Optional.of(sum);
		   	}); // updateStateByKey uses update function to add state

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();

		// Start the computation
		jssc.start();

		// Wait for the computation to terminate
		jssc.awaitTermination();
	}
}