package yearlyawards;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class KeyByCity implements Function<Tuple2<String,Integer>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Tuple2<String, Integer> tuple) throws Exception {
		String key[] = tuple._1().split(",");
		return key[0].toString();
	}

	

}
