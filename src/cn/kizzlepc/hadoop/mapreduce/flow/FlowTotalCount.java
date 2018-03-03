package cn.kizzlepc.hadoop.mapreduce.flow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowTotalCount {
	
	static class FlowTotalMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			String tel = fields[1];
			long upload = Long.parseLong(fields[fields.length-3]);
			long download = Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(tel), new FlowBean(upload, download));
		}
	}
	
	
	static class FlowTotalReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long sumUp = 0;
			long sumDown = 0;
			for(FlowBean bean: values) {
				sumUp += bean.getUpFlow();
				sumDown += bean.getDownFlow();
			}
			FlowBean result = new FlowBean(sumUp, sumDown);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length!=2) {
			args = new String[2];
			args[0] = "hdfs://master:9000/flow/input/data.log";
			args[1] = "hdfs://master:9000/flow/output";
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "flow job");
		job.setJarByClass(FlowTotalCount.class);
		job.setMapperClass(FlowTotalMapper.class);
		job.setReducerClass(FlowTotalReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}
}





















