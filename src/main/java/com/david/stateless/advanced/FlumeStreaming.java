package com.david.stateless.advanced;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.*;
import scala.Tuple2;

import java.nio.charset.Charset;

import java.util.Arrays;


public class FlumeStreaming {
	public static void main(String... args) {

		// Creating a local Streaming Context with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("FlumeWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));

		// Create a DStream that will connect to flume at hostname:port, like localhost:9999
		JavaReceiverInputDStream<SparkFlumeEvent> events = FlumeUtils.createStream(jssc, "localhost", 9999); // Flume String Code Here.

		// Get the lines
		// SparkFlumeEvent.event() -> AvroFlumeEvent
		// AvroFlumeEvent.getBody() -> ByteBuffer
		// ByteBuffer.array() -> byte[]
		JavaDStream<String> lines = events.map( x -> new String(x.event().getBody().array()) );

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