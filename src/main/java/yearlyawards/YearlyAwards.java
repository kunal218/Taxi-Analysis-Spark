package yearlyawards;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.plaf.LabelUI;

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

public class YearlyAwards {
	
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	    
	        
	        	JavaPairRDD<String, Tuple2<Integer, Integer>> mapRDD = inputFile.mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Tuple2<Integer, Integer>> call(String line) throws Exception {
						String[] data = line.split(",");
						Tuple2<Integer, Integer> tuple = new Tuple2<Integer, Integer>(Integer.parseInt(data[11]), 1);
						return new Tuple2<String, Tuple2<Integer,Integer>>(data[7]+","+data[0], tuple);
					}
				})
	        	.reduceByKey(new Function2<Tuple2<Integer,Integer>, Tuple2<Integer,Integer>, Tuple2<Integer,Integer>>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> v1, Tuple2<Integer, Integer> v2) throws Exception {
							int value1 = v1._1+v2._1;
							int value2 = v1._2()+v2._2();
							
						
						return new Tuple2<Integer, Integer>(value1, value2);
					}
				});
	        	JavaRDD<Tuple2<String, Integer>> averageRDD = mapRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>,Tuple2<String, Integer> >() {

						/**
						 * 
						 */
						private static final long serialVersionUID = 4671000078648723812L;

						@Override
						public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> v1)
								throws Exception {
							int average = v1._2()._1 / v1._2()._2();
							return new Tuple2<String, Integer>(v1._1, average);
						}
					})
	        			.sortBy(new Function<Tuple2<String,Integer>, Integer>() {

							/**
							 * 
							 */
							private static final long serialVersionUID = 2135791283642022L;

							@Override
							public Integer call(Tuple2<String, Integer> v1) throws Exception {
								return v1._2();
							}
						}, false, 1);
	        	
	        	
	        	JavaPairRDD<String, Tuple2<String, Integer>> keyedaverage = averageRDD.keyBy(new Function<Tuple2<String,Integer>, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -1444557496196170338L;

					@Override
					public String call(Tuple2<String, Integer> tuple) throws Exception {
						String key [] = tuple._1().split(",");
						return key[0];
					}
				});
	        	int noOfKeys = (int) keyedaverage.keys().distinct().count();
	        	
	        	JavaPairRDD<String, Tuple2<String, Integer>> partRDD = keyedaverage.partitionBy(new MyPartitioner(noOfKeys));
	        
	        	JavaRDD<Tuple2<String, String>> result = partRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Integer>>>, Tuple2<String, String>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = -2513290621917944468L;
					List<Tuple2<String, String>> list = new ArrayList<>();
					@Override
					public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String, Tuple2<String, Integer>>> tuple)
							throws Exception {
						
							for(int i=0;i<10&&tuple.hasNext();i++) {
							Tuple2<String, Tuple2<String, Integer>> record = tuple.next();
							list.add(new Tuple2<String, String>(record._2._1,""+record._2._2 ));
							}
						
						
						return list.iterator();
					}
				});
	        	result.foreach(f->System.out.println(f));
	        		
	        	result.saveAsTextFile("C:\\Users\\GS-2022\\eclipse-workspace\\spark\\YearlyAwards");	
	        		
	        		
	        		
	        		
	        		
	        		
	        		
	        		
	        		
	        		
	        		
	        	
	        
	
			   

	
	}

}
