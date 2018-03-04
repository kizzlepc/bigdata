package cn.kizzlepc.hadoop.mapreduce.provinceflow;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlowProComparator extends WritableComparator {
	public FlowProComparator() {
		// TODO Auto-generated constructor stub
		super(FlowBean.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		// TODO Auto-generated method stub
		System.out.println(a);
		System.out.println(b);
		return 0;
	}
}
