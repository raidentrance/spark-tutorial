/**
 * 
 */
package com.devs4j.spark.rdd.create;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author maagapi
 *
 */
public class CreateRDDFromExistingRDD {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("CreateRDDFromExistingRDD").setMaster("local[3]"));

		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
		JavaRDD<Integer> rdd = sc.parallelize(list);
		JavaRDD<String> rddString = rdd.map(f->f.toString());
		// Use your RDD here
		System.out.println(rddString);
		sc.close();
	}
}
