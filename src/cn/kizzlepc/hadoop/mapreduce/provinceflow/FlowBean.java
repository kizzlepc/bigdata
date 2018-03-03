package cn.kizzlepc.hadoop.mapreduce.provinceflow;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable{
	private long upFlow;
	private long dowFlow;
	private long totalFlow;
	public FlowBean(long upFlow, long dowFlow) {
		this.upFlow = upFlow;
		this.dowFlow = dowFlow;
		this.totalFlow = upFlow + dowFlow;
	}
	public FlowBean() {
	}	
	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDowFlow() {
		return dowFlow;
	}
	public void setDowFlow(long dowFlow) {
		this.dowFlow = dowFlow;
	}
	public long getTotalFlow() {
		return totalFlow;
	}
	public void setTotalFlow(long totalFlow) {
		this.totalFlow = totalFlow;
	}

	@Override
	public String toString() {
		return upFlow + "\t" + dowFlow + "\t" + totalFlow;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		upFlow = in.readLong();
		dowFlow = in.readLong();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(dowFlow);
		out.writeLong(totalFlow);
	}
}
