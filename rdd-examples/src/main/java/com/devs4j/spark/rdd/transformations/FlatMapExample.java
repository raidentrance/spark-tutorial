/**
 * 
 */
package com.devs4j.spark.rdd.transformations;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author maagapi
 *
 */
public class FlatMapExample {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("FlatMapExample").setMaster("local[3]"));
		List<String> phrases = Arrays.asList("This is the first set of words",
							"This is the second set of words", 
							"This is the third set of words", 
							"This is the fourth set of words",
							"This is the last set of words");
		JavaRDD<String> phrasesRdd = sc.parallelize(phrases);
		System.out.printf("The program has %d phrases \n",phrasesRdd.count());
		
		JavaRDD<String> words = phrasesRdd.flatMap(f->Arrays.asList(f.split(" ")).iterator());
		System.out.printf("The program has %d words \n",words.count());
		
		sc.close();
		
	}
}
