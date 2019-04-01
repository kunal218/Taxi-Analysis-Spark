package yearlyawards;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MyMapper implements PairFunction<String, String, Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2302649348692599265L;

	@Override
	public Tuple2<String, Integer> call(String key) throws Exception {
		
		String data[] = key.split(",");
		
	    return new Tuple2<String, Integer>(data[7]+","+data[0], Integer.parseInt(data[11]));
	}
}