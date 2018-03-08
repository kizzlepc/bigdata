package cn.kizzlepc.hadoop.mapreduce.friends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
A:B,C,D,F,E,O
B:A,C,E,K
C:F,A,D,I
D:A,E,F,L
E:B,C,D,M,L
F:A,B,C,D,E,O,M
G:A,C,D,E,F
H:A,C,D,E,O
I:A,O
J:B,O
K:A,C,D
L:D,E,F
M:E,F,G
O:A,H,I,J
 */
public class FriendsMain {
	static class FriendsMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String left = line.split(":")[0];
			String right = line.split(":")[1];
			for(String s:right.split(",")) {
				context.write(new Text(s), new Text(left));
			}
		}
	}
//a o   h o   i o   j o
	static class FriendsReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text value: values) {
				sb.append(value.toString());
				sb.append(",");
			}
			context.write(key,new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "friends");
		job.setJarByClass(FriendsMain.class);
		job.setMapperClass(FriendsMapper.class);
		job.setReducerClass(FriendsReducer.class);
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/hadoop/friends/input/friends.txt"));
		FileOutputFormat.setOutputPath(job, new Path("d:/hadoop/friends/output"));
		
		boolean res = job.waitForCompletion(true);
		System.out.println(res?0:1);
	}

}
