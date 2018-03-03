package cn.kizzlepc.hadoop.mapreduce.provinceflow;

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

public class FlowProvince {
	static class FlowProMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			String tel = fields[1];
			long upFlow = Long.parseLong(fields[fields.length-3]);
			long dowFlow = Long.parseLong(fields[fields.length-2]);
			
			context.write(new Text(tel), new FlowBean(upFlow, dowFlow));
		}
	}
	
	static class FlowProReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long sumUp = 0;
			long sumDown = 0;
			for(FlowBean bean: values) {
				sumUp += bean.getUpFlow();
				sumDown += bean.getDowFlow();
			}
			
			FlowBean resultBean = new FlowBean(sumUp, sumDown);
			context.write(key, resultBean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length!=2) {
			args = new String[2];
			args[0] = "hdfs://master:9000/flow/input/data.log";
			args[1] = "hdfs://master:9000/province/output";
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "province job");
		job.setJarByClass(FlowProvince.class);
		job.setMapperClass(FlowProMapper.class);
		job.setReducerClass(FlowProReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		job.setPartitionerClass(FlowPatitioner.class);
		job.setNumReduceTasks(6);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
