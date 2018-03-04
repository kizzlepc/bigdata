package cn.kizzlepc.hadoop.mapreduce.provinceflow2;

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
		FlowBean a_bean = (FlowBean)a;
		FlowBean b_bean = (FlowBean)b;
		if(a_bean.getTotalFlow()>b_bean.getTotalFlow()) {
			return 1;
		}else if(a_bean.getTotalFlow()<b_bean.getTotalFlow()) {
			return -1;
		}else {
			return 0;
		}
	}
}
