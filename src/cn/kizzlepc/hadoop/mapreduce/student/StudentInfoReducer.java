package cn.kizzlepc.hadoop.mapreduce.student;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StudentInfoReducer extends Reducer<Text, Text, Text, Text> {
	private static final String LEFT_FLAG = "l";//name 1        name l
	private static final String RIGHT_FLAG = "r";//1 class	    class r
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String studentName = null;
		ArrayList<String> studentClassList = new ArrayList<String>();
		
		for(Text val: values) {
			System.out.println(val+"+++++++++++++++++++++++++++++++++++++++++++++++");
			String[] infos = val.toString().split("\t");
			if(infos[1].contains(LEFT_FLAG)) {
				studentName = infos[0];
			}else if(infos[1].contains(RIGHT_FLAG)) {
				studentClassList.add(infos[0]);
			}
		}
		
		for(String studentClass:studentClassList ) {
			context.write(new Text(studentName), new Text(studentClass));
		}
		/*
		for(int i=0; i<studentClassList.size();i++) {
			context.write(new Text(studentName), new Text(studentClassList.get(i)));			
		}*/
	}
}
