package cn.kizzlepc.hadoop.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SecondaryReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
	@Override
	protected void reduce(Text arg0, Iterable<NullWritable> arg1,
			Reducer<Text, NullWritable, NullWritable, Text>.Context arg2) throws IOException, InterruptedException {
		for(NullWritable arg:arg1) {
			arg2.write(NullWritable.get(), arg0);
		}
	}
}
