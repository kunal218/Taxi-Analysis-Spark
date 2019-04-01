package com.spark.taxi;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WeatherSales {

	static class MaxSales implements Comparator<Tuple2<String,Double>>,Serializable{

		/**
		 * 
		 */
		private static final long serialVersionUID = 3998058103580085111L;

		@Override
		public int compare(Tuple2<String, Double> tuple1, Tuple2<String, Double> tuple2) {
			return (int) (tuple1._2()-tuple2._2());
		}
		
	}
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	        
	        
	        JavaPairRDD<String, Double> RDD = inputFile.mapToPair(new PairFunction<String, String, Double>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = -8636575303508814442L;

				@Override
				public Tuple2<String, Double> call(String line) throws Exception {
					String data [] = line.split(",");
					return new Tuple2<String, Double>(data[10],Double.parseDouble(data[6].toString()));
				}
			})
	        
	        .reduceByKey(new Function2<Double, Double, Double>() {
				
				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Double call(Double v1, Double v2) throws Exception {
					return v1+v2;
				}
			});
	        
	        Tuple2<String, Double> result = RDD.map(new Function<Tuple2<String,Double>, Tuple2<String, Double>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = -5798793578404718165L;

				@Override
				public Tuple2<String, Double> call(Tuple2<String, Double> tuple) throws Exception {
					Double value = tuple._2()/90000;
					return new Tuple2<String, Double>(tuple._1(), value);
				}
			})
	        		.max(new MaxSales());
	        
	       System.out.println("Output ---->"+result);
	      
	        
	}

}
