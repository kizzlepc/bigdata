package cn.kizzlepc.hadoop.mapreduce.student;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class StudentInfoMapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String LEFT_FILENAME = "student_info.txt";
	private static final String RIGHT_FILENAME = "student_class_info.txt";
	private static final String LEFT_FLAG = "l";
	private static final String RIGHT_FLAG = "r";
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String flag = null;
		String mapValue = null;
		String mapKey = null;
		
		
		FileSplit fs = (FileSplit)context.getInputSplit();
		String filePath = fs.getPath().toString();
		System.out.println(value+"========================================================");
		if(filePath.contains(LEFT_FILENAME)) {
			flag = LEFT_FLAG;
			mapValue = value.toString().split(" ")[0];
			mapKey = value.toString().split(" ")[1];
		}else if(filePath.contains(RIGHT_FILENAME)){
			flag = RIGHT_FLAG;
			mapValue = value.toString().split(" ")[1];
			mapKey = value.toString().split(" ")[0];
		}
		
		
		context.write(new Text(mapKey), new Text(mapValue+"\t"+flag));
	}
}
