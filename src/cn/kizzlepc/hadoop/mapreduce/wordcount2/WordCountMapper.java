package cn.kizzlepc.hadoop.mapreduce.wordcount2;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private String pattern = "[\\p{P}&&[^-_]]";
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String new_line = line.replaceAll(pattern, " ");
		//默认分隔符是空格
		StringTokenizer stringTokenizer = new StringTokenizer(new_line);
		//是否还有分隔符
		while(stringTokenizer.hasMoreTokens()) {
			//取当前位置到下一个分隔符的字符串
			word.set(stringTokenizer.nextToken());
			context.write(word, one);
		}
	}
}
