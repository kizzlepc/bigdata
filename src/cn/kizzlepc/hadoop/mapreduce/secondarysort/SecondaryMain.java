package cn.kizzlepc.hadoop.mapreduce.secondarysort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class SecondaryMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2) {
			//System.err.println("Usage: wordcount <in> <out>");
			//System.exit(2);
			args = new String[2];
			args[0] = "hdfs://master:9000/secondarysort/input/data.txt";
			args[1] = "hdfs://master:9000/secondarysort/output1";
		}
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "secondarysort count");
		job.setJarByClass(SecondaryMain.class);
		job.setMapperClass(SecondaryMapper.class);
		job.setReducerClass(SecondaryReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		job.setNumReduceTasks(1);
		job.setGroupingComparatorClass(SecondaryGroupComparator.class);
		job.setPartitionerClass(SecondaryPatitioner.class);
		job.setSortComparatorClass(SeconderSortComparator.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.out.println(job.waitForCompletion(true)?0:1);		
	}

}
