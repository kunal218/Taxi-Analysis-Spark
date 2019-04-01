package yearlyawards;

import org.apache.spark.api.java.function.Function2;


public class MyReducer implements Function2<Integer, Integer, Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 3128775767157821273L;

	@Override
	public Integer call(Integer v1, Integer v2) throws Exception {
		
		return v1+v2;
	}
	
}
 
