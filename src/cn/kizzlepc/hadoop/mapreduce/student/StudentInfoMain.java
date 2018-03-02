package cn.kizzlepc.hadoop.mapreduce.student;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentInfoMain {

	public static void main(String[] args) throws Exception {
		if(args.length != 2) {
			//System.err.println("Usage: <in> <out>");
			//System.exit(2);
			args = new String[2];
			args[0] = "hdfs://master:9000/student/input";
			args[1] = "hdfs://master:9000/student/output";
		}
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "student job");
		job.setJarByClass(StudentInfoMain.class);
		job.setMapperClass(StudentInfoMapper.class);
		job.setReducerClass(StudentInfoReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
