package com.xl.sparksql.java;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSparkSQLExample {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("Java Spark SQL basic example")
				.config("spark.some.config.option", "some-value").getOrCreate();

		Dataset<Row> df = spark.read().json("src/main/resources/com/xl/sparksql/java/people.json");

		df.show(1);
		
		df.printSchema();
		
		df.select("name").show();

		df.select(col("name"), col("salary").plus(1)).show();
		
		df.createOrReplaceTempView("people");
		
		Dataset<Row> sqlDF = spark.sql("SELECT * FROM people");
		sqlDF.show();
	}

}
