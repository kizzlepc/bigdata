package cn.kizzlepc.hadoop.mapreduce.totalsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

public class TotalSortMain {
	public static void main(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
		Path outPath = new Path(args[1]);
		Path partitionFile = new Path(args[2]);
		int reduceNum = Integer.parseInt(args[3]);
		
		RandomSampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(0.1, 100000, 10);
		
		Configuration conf = new Configuration();
		TotalOrderPartitioner.setPartitionFile(conf, partitionFile);
		
		Job job = Job.getInstance(conf,"totalsort job");
		job.setJarByClass(TotalSortMain.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(reduceNum);
		job.setPartitionerClass(TotalOrderPartitioner.class);
		
		FileInputFormat.setInputPaths(job, inputPath);
		FileOutputFormat.setOutputPath(job, outPath);
		outPath.getFileSystem(conf).delete(outPath, true);
		
		InputSampler.writePartitionFile(job, sampler);
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
