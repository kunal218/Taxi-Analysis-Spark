package commutetime;

import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class KeyByCity implements Function<Tuple2<String,Double>, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String call(Tuple2<String, Double> key) throws Exception {
		String key1[] = key._1().split(",");
		return key1[0].toString();
	}

}
