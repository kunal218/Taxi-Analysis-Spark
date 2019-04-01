package commutetime;

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

public class MostCommuteTime {
	//static int index=-1;
	
	
		
		
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("taxi review");
		 System.setProperty("hadoop.home.dir", "C:\\winutils");
	        
		 	JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

	        JavaRDD<String> inputFile = sparkContext.textFile("TaxiDataset2.txt");
	      JavaPairRDD<String, Tuple2<String, Double>> mapRDD = inputFile.mapToPair(new MyMapper())
	    		  .reduceByKey(new MyReducer())
	    .sortByKey()
	    .keyBy(new KeyByCity());
	    
	      int noOfKeys = (int) mapRDD.keys().distinct().count();
	    	
	     
	    JavaPairRDD<String, Tuple2<String, Double>> partRDD = mapRDD.partitionBy(new MyPartitioner(noOfKeys));
	    		 
	    	JavaRDD<Tuple2<String, Tuple2<String, Double>>> result = partRDD.mapPartitions(new MyMapPartitioner() ); 
	    		
	      result.foreach(f->System.out.println(f));
	    	result.saveAsTextFile("C:\\Users\\GS-2022\\eclipse-workspace\\spark\\CommuteTime");	  
	}

}
