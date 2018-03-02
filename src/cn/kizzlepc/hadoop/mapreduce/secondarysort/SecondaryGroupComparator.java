package cn.kizzlepc.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondaryGroupComparator extends WritableComparator {
	public SecondaryGroupComparator() {
		super(Text.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		int a0 = Integer.parseInt(a.toString().split(" ")[0]);
		int b0 = Integer.parseInt(b.toString().split(" ")[0]);
		System.out.println(a0);
		System.out.println(b0);
		if(a0>b0) {
			return 1;
		}else if(a0<b0) {
			return -1;
		}else {
			return 0;
		}
	}
}
