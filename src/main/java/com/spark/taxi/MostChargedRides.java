package com.spark.taxi;

import java.util.ArrayList;
import java.util.Arrays;
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

public class MostChargedRides {

	static int index=-1;
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	        
	    
	        
	       JavaRDD<Tuple2<String, Double>> mapRDD = inputFile.map(new Function<String, Tuple2<String, Double>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Tuple2<String, Double> call(String line) throws Exception {
					String data[] = line.split(",");
					return new Tuple2<String, Double>(data[7].toString(), Double.parseDouble(data[6]));
				}
			})
	    		  
	    		   .sortBy(new Function<Tuple2<String,Double>, Double>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Double call(Tuple2<String, Double> value) throws Exception {
						return value._2;
					}
				}, false, 1);
	       
	    	   
	       JavaPairRDD<String, Tuple2<String, Double>> keyedRDD = mapRDD.keyBy(new Function<Tuple2<String,Double>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Double> value) throws Exception {
				// TODO Auto-generated method stub
				return value._1;
			}
		});
	
	 int noOfKeys = (int) keyedRDD.keys().distinct().count();
	 
	 
	 	
	 
	JavaPairRDD<String,Tuple2<String,Double>> partRDD = keyedRDD.partitionBy(new Partitioner() {
		Map<String, Integer> keyMap = new HashMap<String, Integer>();
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public int numPartitions() {
			// TODO Auto-generated method stub
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
	
	
	JavaRDD<Tuple2<String, Double>> resultRDD = partRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Double>>>, Tuple2<String, Double>>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = -2167684256082115002L;

		@Override
		public Iterator<Tuple2<String, Double>> call(Iterator<Tuple2<String, Tuple2<String, Double>>> tuple1)
				throws Exception {
			List<Tuple2<String, Double>> list = new ArrayList<>();
		
					for(int i=0;i<10&&tuple1.hasNext();i++) {
						
						list.add(tuple1.next()._2);
					}
				
				System.out.println("SIZE : ------> "+list.size());
			return list.iterator();
		}
	});
			
			
				
					
	resultRDD.foreach(f -> System.out.println(f)); 
	resultRDD.saveAsTextFile("C:\\Users\\GS-2022\\eclipse-workspace\\spark\\top10chrge");
	}

}
