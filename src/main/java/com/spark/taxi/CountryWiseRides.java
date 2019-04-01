package com.spark.taxi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class CountryWiseRides {

	
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	        
	    
	        
	       JavaPairRDD<String,Tuple2<String,Integer>> mapRDD = inputFile.map(new Function<String, Tuple2<String, Integer>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
					String data[] = line.split(",");
					return new Tuple2<String, Integer>(data[9].toString(), 1);
				}
			})
	    		   .keyBy(new Function<Tuple2<String,Integer>, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 7706123520076727988L;

					@Override
					public String call(Tuple2<String, Integer> value) throws Exception {
						return value._1;
					}
				});
	       
	       JavaPairRDD<Integer, Tuple2<String, Tuple2<String, Integer>>> resultRDD = mapRDD.reduceByKey(new Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {
			int sum=0;
			/**
			 * 
			 */
			private static final long serialVersionUID = 6854632915649057610L;

			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
			
				sum=value1._2+value2._2;
				return new Tuple2<String, Integer>(value1._1,sum);
			}
		})
	       .keyBy(new Function<Tuple2<String,Tuple2<String,Integer>>, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 6551995592436240266L;

			@Override
			public Integer call(Tuple2<String, Tuple2<String, Integer>> value) throws Exception {
				return value._2._2;
			}
		})
	       .sortByKey(false);
	       resultRDD.foreach(f -> System.out.println(f)); 
	       
	       
	      
	}

}
