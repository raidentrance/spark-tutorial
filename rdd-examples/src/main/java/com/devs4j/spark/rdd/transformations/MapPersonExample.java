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
class Person {
	private String name;
	private String lastName;

	public Person(String name, String lastName) {
		this.name = name;
		this.lastName = lastName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

}

public class MapPersonExample {
	public static void main(String[] args) {
		JavaSparkContext sc = new JavaSparkContext(
				new SparkConf().setAppName("FilterNamesExample").setMaster("local[3]"));
		List<String> names = Arrays.asList("Alex,Bautista", "Pedro,Lopez", "Arturo,Mendez", "Juan,Sánchez",
				"Pancho,Pantera", "Jon,Smith");
		JavaRDD<String> peopleRdd = sc.parallelize(names);
		JavaRDD<Person> customPeople = peopleRdd.map(p -> new Person(p.split(",")[0], p.split(",")[1]));
		System.out.println(customPeople);
		sc.close();
	}
}
