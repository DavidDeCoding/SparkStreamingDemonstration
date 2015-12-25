package com.david.stateless.basic;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import org.apache.spark.api.java.*;

import java.util.Arrays;

public class JoiningQuickStreaming {
	public static void main(String... args) {

		// Creating a local Streaming Context with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("JoiningWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));
		JavaSparkContext jsc = jssc.sparkContext();

		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> streamLines = jssc.socketTextStream("localhost", 9999);
		JavaRDD<String> storedLines = jsc.textFile("./files/textfile.txt");

		// Split each line into words
		JavaDStream<String> streamWords = streamLines.flatMap ( x -> Arrays.asList( x.split(" ")));
		JavaRDD<String> storedWords = storedLines.flatMap ( x -> Arrays.asList( x.split(" ")));

		// Count each word in each batch
		JavaPairDStream<String, Integer> streamPairs = streamWords.mapToPair( x -> new Tuple2(x, 1));
		JavaPairDStream<String, Integer> streamWordCounts = streamPairs.reduceByKey( (i1, i2) -> i1 + i2 );
		JavaPairRDD<String, Integer> storedPairs = storedWords.mapToPair( x -> new Tuple2(x, 1));
		JavaPairRDD<String, Integer> storedWordCounts = storedPairs.reduceByKey( (i1, i2) -> i1 + i2 );

		// Joined Word Counts, here we join dataset to stream
		JavaPairDStream<String, Tuple2<Integer, Integer>> joinedWordCounts = streamWordCounts.transformToPair( rdd -> rdd.join( storedWordCounts ));

		// Total WordCount
		JavaPairDStream<String, Integer> wordCounts = joinedWordCounts.mapToPair( t -> new Tuple2(t._1(), (Integer)t._2()._1() + (Integer)t._2()._2()));

		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();

		// Start the computation
		jssc.start();

		// Wait for the computation to terminate
		jssc.awaitTermination();

	}
}