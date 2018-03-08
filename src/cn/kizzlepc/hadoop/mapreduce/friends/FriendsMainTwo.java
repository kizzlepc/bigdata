package cn.kizzlepc.hadoop.mapreduce.friends;

import java.io.IOException;
import java.util.ArrayList;

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

import cn.kizzlepc.hadoop.mapreduce.friends.FriendsMain.FriendsMapper;
import cn.kizzlepc.hadoop.mapreduce.friends.FriendsMain.FriendsReducer;
/**
A	I,K,C,B,G,F,H,O,D,
B	A,F,J,E,
C	A,E,B,H,F,G,K,
D	G,C,K,A,L,F,E,H,
E	G,M,L,H,A,F,B,D,
F	L,M,D,C,G,A,
G	M,
H	O,
I	O,C,
J	O,
K	B,
L	D,E,
M	E,F,
O	A,H,I,J,F,
 */
public class FriendsMainTwo {
	static class FriendsTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> fs = new ArrayList<String>();
			String line = value.toString();
			String[] fields = line.split("\t");
			String left = fields[0];
			String rights = fields[1];
			for(String right: rights.split(",")) {
				fs.add(right);
			}
			fs.sort(null);
			for(int i=0;i< fs.size()-2;i++) {
				for(int j=i+1;j<fs.size()-1;j++) {
					context.write(new Text(fs.get(i)+"--"+fs.get(j)), new Text(left));
				}
			}
		}
			
	}
	//a-b c
	static class FriendsTwoReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for(Text value: values) {
				sb.append(value.toString());
				sb.append(",");
			}
			context.write(key, new Text(sb.toString()));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "friends");
		job.setJarByClass(FriendsMainTwo.class);
		job.setMapperClass(FriendsTwoMapper.class);
		job.setReducerClass(FriendsTwoReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("d:/hadoop/friends/output/part-r-00000"));
		FileOutputFormat.setOutputPath(job, new Path("d:/hadoop/friends/output1"));
		
		boolean res = job.waitForCompletion(true);
		System.out.println(res?0:1);
	}

}
