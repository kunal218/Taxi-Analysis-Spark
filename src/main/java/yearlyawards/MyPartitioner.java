package yearlyawards;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapred.Partitioner;
import org.apache.spark.Partition;

public class MyPartitioner extends org.apache.spark.Partitioner {
	/**
	 * 
	 */
	private static final long serialVersionUID = -2590418116662770223L;
	int noOfKeys = 0;
	static int index=-1;
	public MyPartitioner(int keys) {
		this.noOfKeys=keys;
	}
	static Map<String, Integer> keyMap = new HashMap<String, Integer>();
	@Override
	public int numPartitions() {
		return noOfKeys;
	}
	
	@Override
	public int getPartition(Object key) {
		if (keyMap.containsKey(key.toString())) {
			System.out.println("key Found"+key.toString());
			return keyMap.get(key.toString());
		} else {
			System.out.println("Putting Key ->"+key.toString()+"Index"+(++index));
			keyMap.put(key.toString(), index);
		}
		return keyMap.get(key.toString());	
	}

}
