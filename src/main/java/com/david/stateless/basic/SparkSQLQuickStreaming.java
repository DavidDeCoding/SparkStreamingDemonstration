package com.david.stateless.basic;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import scala.Tuple2;

import java.util.Arrays;

import com.david.extra.Word;

public class SparkSQLQuickStreaming {

	public static void main(String... args) {

		// Creating a local Streaming Context with two working thread and batch interval of 1 second
		SparkConf conf = new SparkConf()
							.setMaster("local[2]")
							.setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

		// Create a DStream that will connect to hostname:port, like localhost:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);

		// Split each line into words
		JavaDStream<String> words = lines.flatMap ( x -> Arrays.asList( x.split(" ")));

		// Using Spark SQL
		words.foreachRDD( (rdd, time) -> {
			SQLContext sqlContext = JavaSQLContextSingleton.getInstance(rdd.context());

			// Register as table
			JavaRDD<Word> rowRDD = rdd.map ( x -> {
				Word word = new Word();
				word.setWord( x );
				return word;
			});
			DataFrame wordsDataFrame = sqlContext.createDataFrame(rowRDD, Word.class);
			wordsDataFrame.registerTempTable("words");

			// Do word count on table using SQL and print it
			DataFrame wordCounts = sqlContext.sql("select word, count(*) as total from words group by word");

			// Print the elements of each RDD generated in this DStream to the console
			wordCounts.show();

			return null;
		});

		// Start the computation
		jssc.start();

		// Wait for the computation to terminate
		jssc.awaitTermination();
	}
}

class JavaSQLContextSingleton {
	private static transient SQLContext instance = null;
	public static SQLContext getInstance(SparkContext sparkContext) {
		if (instance == null) instance = new SQLContext(sparkContext);
		return instance;
	}
}