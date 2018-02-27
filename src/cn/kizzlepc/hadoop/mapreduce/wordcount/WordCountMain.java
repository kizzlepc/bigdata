package cn.kizzlepc.hadoop.mapreduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountMain {
	public static void main(String[] args) throws Exception {
		if(args == null || args.length == 0) {
			args = new String[2];
			args[0] = "hdfs://master:9000/wordcount/input/english.txt";
			args[1] = "hdfs://master:9000/wordcount/output";
		}	
		Configuration conf = new Configuration();
				
		Job job = Job.getInstance(conf);
		
		//job.setJar("/home/hadoop/wordcount.jar");
		job.setJarByClass(WordCountMain.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean res = job.waitForCompletion(true);
		System.out.println(res?0:1);
	}
}
