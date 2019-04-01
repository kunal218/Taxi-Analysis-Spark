package yearlyawards;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;

public class MyMapPartitioner implements FlatMapFunction<Iterator<Tuple2<String,Tuple2<String,Integer>>>, Tuple2<String, String>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6294625047329581613L;
	List<Tuple2<String, String>> list = new ArrayList<>();
	
	@Override
	public Iterator<Tuple2<String, String>> call(Iterator<Tuple2<String, Tuple2<String, Integer>>> tuple) throws Exception {
		if(tuple.hasNext()) {
		Tuple2<String, Tuple2<String, Integer>> record = tuple.next();
		
		list.add(new Tuple2<String, String>(record._2._1,""+record._2()._2()));
		
		}
		return list.iterator();
	}
	

}
