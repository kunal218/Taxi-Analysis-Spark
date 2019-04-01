package com.spark.taxi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.Accumulator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class HourlyBusinessAreas {
	
	static class SortTrips implements Comparator<Tuple2<String,Integer>>,Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = -8276570888856281403L;

		@Override
		public int compare(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) {
			return tuple2._2()-tuple1._2();
		}
		
	}
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		 			
	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	      
	        
	        
	       JavaPairRDD<String, Integer> mapRDD = inputFile.mapToPair(new PairFunction<String, String, Integer>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = -5970406568042399763L;

				@Override
				public Tuple2<String, Integer> call(String line) throws Exception {
					String data [] = line.split(",");
					String time [] = data[2].split(" ");
					String hour [] = time[1].split(":");
					return new Tuple2<String, Integer>(hour[0]+","+data[1],1);
				}
			})
	        .sortByKey();
	        
	       
	       
	       JavaPairRDD<String, Integer> reducedRDD = mapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = -6223655352748332762L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1+v2;
			}
			
			
		});
	       
	    	
	       	    		 
	       
	       
	       
	       JavaPairRDD<String, Tuple2<Integer, String>> reversedRDD = reducedRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
				return new Tuple2<Integer, String>(tuple._2(), tuple._1());
			}
		}).keyBy(new Function<Tuple2<Integer,String>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<Integer, String> value) throws Exception {
				String [] data = value._2.split(",");
				return data[0].toString();
			}
		})
	       .sortByKey(false);
	       
	       
	       
	       
	       
	       
	       
	       
	       int noOfKeys= (int) reversedRDD.keys().distinct().count();
	       
	       System.out.println("keys ------->"+noOfKeys);
	       
	   JavaPairRDD<String, Tuple2<Integer, String>> hashRDD = reversedRDD.partitionBy(new HashPartitioner(noOfKeys));
	    
	     
	  JavaRDD<Tuple3<String, String, Integer>> result = hashRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<Integer,String>>>, Tuple3<String, String, Integer>>() {
		   List<Tuple3<String,String, Integer>> list = new ArrayList<>();
			/**
			 * 
			 */
			private static final long serialVersionUID = -6882465398707986740L;

			@Override
			public Iterator<Tuple3<String, String, Integer>> call(
					Iterator<Tuple2<String, Tuple2<Integer, String>>> tuple1) throws Exception {
			
				Tuple2<String, Tuple2<Integer, String>> tuple = tuple1.next();
				list.add(new Tuple3<String, String, Integer>(tuple._1(), tuple._2()._2(),tuple._2()._1()));
				return list.iterator();
			}
		});
	    		
	   
	     
	     result.foreach(f -> System.out.println(f)); 	       
	     result.saveAsTextFile("C:\\Users\\GS-2022\\eclipse-workspace\\spark\\busiestAreas");
	}

}
