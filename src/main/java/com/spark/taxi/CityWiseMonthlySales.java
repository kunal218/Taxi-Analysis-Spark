package com.spark.taxi;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

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
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class CityWiseMonthlySales {
	static int index  = -1;
	
	
	public static void main(String[] args) {
		 SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	        JavaRDD<String> wordsFromFile = inputFile.flatMap(new FlatMapFunction<String, String>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = -2503237127505166949L;

				public Iterator<String> call(String line) throws Exception {
				return Arrays.asList(line.split("\\n")).iterator();
				}
			});
	     
	       
	        
	   
	      
	JavaPairRDD<String, Double> pairRDD = wordsFromFile.mapToPair(new PairFunction<String, String, Double>() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, Double> call(String line) throws Exception {
			String data[] = line.split(",");
			String date[]= data[2].split(" ");
			String month[]= date[0].split("-");
			
			return new Tuple2<String, Double>(data[7].toString()+","+month[1].toString(), Double.parseDouble(data[6]));
		}
	});
	   
	
	JavaPairRDD<String, Tuple2<String, Double>> reducedRDD = pairRDD.reduceByKey(new Function2<Double, Double, Double>() {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -1946879774065786299L;

		@Override
		public Double call(Double v1, Double v2) throws Exception {
			
			return v1+v2;
		}
	})
		.keyBy(new Function<Tuple2<String,Double>, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String call(Tuple2<String, Double> value) throws Exception {
				String key[] =value._1.split(",");
				return key[0].toString();
			}
		})	.sortByKey();
	
	
	int noOfKeys = (int) reducedRDD.keys().distinct().count();
	System.out.println("No of keys -.>....."+noOfKeys);
	//JavaPairRDD<String, Double> partitionedRDD = reducedRDD.partitionBy(new HashPartitioner(noOfKeys));
	
		JavaPairRDD<String, Tuple2<String, Double>> partitionedRDD = reducedRDD.partitionBy(new Partitioner() {
			/**
			 * 
			 */
			
			private static final long serialVersionUID = -2228005145690654670L;
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
	
	     
	      partitionedRDD.foreach(f -> System.out.println(f));
	      partitionedRDD.saveAsTextFile("C:\\Users\\GS-2022\\eclipse-workspace\\spark\\sparkTAXI");
	 
	}
}
