package com.xl.spark.java;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class JavaWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");
	private static String[] arg;

	public static void main(String[] args) throws Exception {

		arg = args;

		if (args.length < 1) {
			System.err.println("Usage: JavaWordCount <file>");
			System.exit(1);
		}

		SparkSession spark = SparkSession.builder().appName("JavaWordCount").getOrCreate();

		// sparkCoreTest(spark);

		sparkCoreParallizeTest(spark);

		stopSparkSession(spark);
	}

	private static void stopSparkSession(SparkSession spark) {
		spark.stop();

	}

	private static void sparkCoreParallizeTest(SparkSession spark) {
		JavaRDD<String> lines = spark.read().textFile(arg[0]).javaRDD();

		// JavaRDD<String> words = lines.flatMap(s ->
		// Arrays.asList(SPACE.split(s)).iterator());
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			public Iterator<String> call(String s) throws Exception {
				return Arrays.asList(s.split(",")).iterator();
			}
		});

		List<String> output = words.collect();
		for (String tuple : output) {
			System.out.println(tuple);
		}

	}

	private static void sparkCoreTest(SparkSession spark) {
		JavaRDD<String> lines = spark.read().textFile(arg[0]).javaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}

		counts.saveAsTextFile(arg[1]);

	}

}
