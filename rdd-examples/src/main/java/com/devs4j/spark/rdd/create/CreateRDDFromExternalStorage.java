/**
 * 
 */
package com.devs4j.spark.rdd.create;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author maagapi
 *
 */
public class CreateRDDFromExternalStorage {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("WordCount").setMaster("local[3]"));
		JavaRDD<String> rdd = sc.textFile("src/main/resources/words.txt");
		// Use your RDD here
		System.out.println(rdd);
		sc.close();
	}
}
