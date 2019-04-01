package commutetime;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class MyMapPartitioner implements FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Double>>>, Tuple2<String, Tuple2<String, Double>>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6294625047329581613L;
	List<Tuple2<String, Tuple2<String, Double>>> list = new ArrayList<>();
	@Override
	public Iterator<Tuple2<String, Tuple2<String, Double>>> call(Iterator<Tuple2<String, Tuple2<String, Double>>> tuple) throws Exception {
		if(tuple.hasNext()) {
		Tuple2<String, Tuple2<String, Double>> record = tuple.next();
		
		list.add(record);
		System.out.println("records - >>>>>"+record);}
		return list.iterator();
	}
	

}
