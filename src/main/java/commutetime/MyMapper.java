package commutetime;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class MyMapper implements PairFunction<String, String, Double>{

	/**
	 * 
	 */
	private static final long serialVersionUID = -2302649348692599265L;

	@Override
	public Tuple2<String, Double> call(String line) throws Exception {
		
		String data[] = line.split(",");
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
	    Date parsedDate1 = dateFormat.parse(data[2].toString());
	    Date parsedDate2 = dateFormat.parse(data[4].toString());
	    
	    Timestamp timestamp1 = new java.sql.Timestamp(parsedDate1.getTime());
	    Timestamp timestamp2 = new java.sql.Timestamp(parsedDate2.getTime());
	    
	    long difference = timestamp2.getTime()-timestamp1.getTime();
	    
		return new Tuple2<String, Double>(data[7]+","+data[1]+","+data[3], (double) difference);
	}
}