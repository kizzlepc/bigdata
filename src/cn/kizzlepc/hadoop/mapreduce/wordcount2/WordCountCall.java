package cn.kizzlepc.hadoop.mapreduce.wordcount2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountCall {

	public static void main(String[] args) throws Exception {
		if(args.length!=2) {
			//System.err.println("Usage: wordcount <in> <out>");
			//System.exit(2);
			args = new String[2];
			args[0] = "hdfs://master:9000/wordcount/input/english.txt";
			args[1] = "hdfs://master:9000/wordcount/input19";
		}
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountCall.class);
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
