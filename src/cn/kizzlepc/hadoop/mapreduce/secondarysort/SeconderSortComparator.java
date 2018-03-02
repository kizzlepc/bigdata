package cn.kizzlepc.hadoop.mapreduce.secondarysort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SeconderSortComparator extends WritableComparator {
	public SeconderSortComparator() {
		super(Text.class, true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		int a0 = Integer.parseInt(a.toString().split(" ")[0]);
		int b0 = Integer.parseInt(b.toString().split(" ")[0]);
		int a1 = Integer.parseInt(a.toString().split(" ")[1]);
		int b1 = Integer.parseInt(b.toString().split(" ")[1]);
		
		if(a0==b0) {
			if(a1>b1) {
				return 1;
			}else if(a1<b1) {
				return -1;
			}else {
				return 0;
			}
		}else {
			if(a0>b0) {
				return 1;
			}else {
				return -1;
			}
		}
	}
}
