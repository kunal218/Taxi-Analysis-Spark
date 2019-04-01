package commutetime;

import org.apache.spark.api.java.function.Function2;


public class MyReducer implements Function2<Double, Double, Double>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3128775767157821273L;

	@Override
	public Double call(Double v1, Double v2) throws Exception {
		Double value = (v1+v2)/20000;
		return value;
	}
	
}
 
