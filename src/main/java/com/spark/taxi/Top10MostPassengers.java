package com.spark.taxi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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

public class Top10MostPassengers {
	static int index=-1;
	static class sortbypassengers implements Comparator<Tuple3<String, String, Integer>>{

		@Override
		public int compare(Tuple3<String, String, Integer> tuple1, Tuple3<String, String, Integer> tuple2) {
			return tuple2._3()-tuple1._3();
		}
		
	}
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	        
	        JavaPairRDD<String, Tuple2<String, Integer>> mappedRDD = inputFile.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Tuple2<String, Integer>> call(String line) throws Exception {
					String data[] = line.split(",");
					return new Tuple2<String, Tuple2<String,Integer>>(data[7].toString()+","+data[0].toString(), new Tuple2<String, Integer>(line, Integer.parseInt(data[5])));
				}
			});
	        
	      JavaPairRDD<String,Tuple2<String,Tuple2<String,Integer>>> reducedRDD = mappedRDD.reduceByKey(new Function2<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple2<String,Integer>>() {
				
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) throws Exception {
					int sum = v1._2()+v2._2();
					return new Tuple2<String, Integer>(v1._1(),sum);
				}
			})
	    		  .keyBy(new Function<Tuple2<String,Tuple2<String,Integer>>, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -5757880759617873710L;

					@Override
					public String call(Tuple2<String, Tuple2<String, Integer>> tuple) throws Exception {
						String key[] = tuple._1().split(",");
						return key[0].toString();
					}
				})
	    		  .sortByKey(true);
	      
	      
	    		 
	        int noOfKeys = (int) reducedRDD.keys().distinct().count();
	       
	        
	      JavaPairRDD<String, Tuple2<String, Tuple2<String, Integer>>> partRDD = reducedRDD.partitionBy(new Partitioner() {
	        	/**
				 * 
				 */
				private static final long serialVersionUID = 1L;
				Map<String, Integer> keyMap = new HashMap<String, Integer>();
				@Override
				public int numPartitions() {
					return noOfKeys;
				}
				
				@Override
				public int getPartition(Object key) {
					if (keyMap.containsKey(key.toString())) {

						return keyMap.get(key.toString());
					} else
						keyMap.put(key.toString(), ++index);

					return keyMap.get(key.toString());	
				}
			});
	      
	
	     JavaRDD<Tuple3<String, String, Integer>> result = partRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Tuple2<String,Integer>>>>, Tuple3<String, String, Integer>>() {

			
			/**
			 * 
			 */
			private static final long serialVersionUID = -6358710873326039420L;

			@Override
			public Iterator<Tuple3<String, String, Integer>> call(Iterator<Tuple2<String, Tuple2<String, Tuple2<String, Integer>>>> tuple2)
					throws Exception {
				List<Tuple3<String,String, Integer>> list = new ArrayList<>();
			
				while(tuple2.hasNext()) {
					Tuple2<String, Tuple2<String, Tuple2<String, Integer>>> element = tuple2.next();
					String[] driver = element._2()._1.split(",");
					Tuple3<String, String, Integer> tuple = 
							new Tuple3<String, String, Integer>(element._1(),driver[1].toString(),element._2()._2()._2);
					list.add(tuple);
				//System.out.println("Tuple Added ------->"+tuple);
				
				}
			
					Collections.sort(list, new sortbypassengers());
				
					List<Tuple3<String, String, Integer>> resultList = list.subList(0, 10);
					return resultList.iterator();
			}
		});
	      
	        
	        result.foreach(f -> System.out.println(f)); 
	        result.saveAsTextFile("C:\\\\Users\\\\GS-2022\\\\eclipse-workspace\\\\spark\\\\top10mostpass");
	}

}
