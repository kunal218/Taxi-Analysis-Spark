package commutetime;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.spark.Partition;

public class MyPartitioner extends org.apache.spark.Partitioner {
	static Map<String, Integer> keyMap = new HashMap<String, Integer>();
	static int index=-1;
	/**
	 * 
	 */
	private static final long serialVersionUID = -2590418116662770223L;
	int noOfKeys = 0;
	public MyPartitioner(int keys) {
		this.noOfKeys=keys;
	}
	@Override
	public int numPartitions() {
		return noOfKeys;
	}
	
	@Override
	public int getPartition(Object key) {
		if (keyMap.containsKey(key.toString())) {
			System.out.println("Map IF -> "+keyMap);
			System.out.println("key Found"+key.toString());
			return keyMap.get(key.toString());
		} else {
			System.out.println("Putting Key ->"+key.toString()+"Index"+(++index));
			System.out.println("Map else -> "+keyMap);
			
			keyMap.put(key.toString(), index);
		}
		return keyMap.get(key.toString());	
	}

}
