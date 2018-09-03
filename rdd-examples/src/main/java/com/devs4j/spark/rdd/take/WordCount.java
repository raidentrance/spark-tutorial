/**
 * 
 */
package com.devs4j.spark.rdd.take;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author raidentrance
 *
 */
public class WordCount {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount").setMaster("local[3]"));
		JavaRDD<String> lines = sc.textFile("src/main/resources/words.txt");
		JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		Map<String, Long> wordCounts = words.countByValue();

		for (Entry<String, Long> wordCount : wordCounts.entrySet()) {
			System.out.printf("%s - %d \n", wordCount.getKey(), wordCount.getValue());
		}
		sc.close();
	}
}
