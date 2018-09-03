package com.devs4j.spark.rdd.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FilterNamesExample {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("FilterNamesExample").setMaster("local[3]"));
		List<String> names = Arrays.asList("Alex", "Pedro", "", "Juan", "Pancho", "");
		JavaRDD<String> namesRdd = sc.parallelize(names);
		System.out.printf("Names before filter %d \n", namesRdd.count());
		namesRdd = namesRdd.filter(f->!f.isEmpty());
		System.out.printf("Names after filter %d \n", namesRdd.count());
		sc.close();
	}
}
