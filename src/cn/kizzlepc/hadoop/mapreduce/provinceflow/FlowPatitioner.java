package cn.kizzlepc.hadoop.mapreduce.provinceflow;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class FlowPatitioner extends Partitioner<Text, FlowBean> {
	
	@Override
	public int getPartition(Text key, FlowBean value, int num) {
		System.out.println("===================================");
		String prefix = key.toString().substring(0, 3);
		Integer provinceId = provinceDict(prefix);
		return provinceId;
	}

	private Integer provinceDict(String prefix) {
		HashMap<String, Integer>  hm = new HashMap<String, Integer>();
		hm.put("135", 0);
		hm.put("136", 1);
		hm.put("137", 2);
		hm.put("138", 3);
		hm.put("139", 4);
		return hm.get(prefix)==null?5:hm.get(prefix);
	}
	
}
