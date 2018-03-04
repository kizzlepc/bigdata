package cn.kizzlepc.hadoop.mapreduce.provinceflow2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 
 * @author admin
 *result:
 *[hadoop@master ~]$ hadoop fs -cat /province/output/part-r-00005
	13480253104	180		180		360
	15989002119	1938	180		2118
	18211575961	1527	2106	3633
	84138413	4116	1432	5548
	15920133257	3156	2936	6092
	15013685858	3659	3538	7197
	18320173382	9531	2412	11943
	[hadoop@master ~]$ hadoop fs -cat /province/output/part-r-00004
	13926251106	240		0		240
	13926435656	132		1512	1644
	13922314466	3008	3720	6728
	13925057413	11058	48243	59301
	[hadoop@master ~]$ hadoop fs -cat /province/output/part-r-00003
	13826544101	264		0		264
	[hadoop@master ~]$ hadoop fs -cat /province/output/part-r-00002
	13719199419	360		120		480
	13726238888	4962	49362	54324
	[hadoop@master ~]$ hadoop fs -cat /province/output/part-r-00001
	13602846565	1938	2910	4848
	13660577991	6960	690		7650
	[hadoop@master ~]$ hadoop fs -cat /province/output/part-r-00000
	13560439658	2232	1908	4140
	13560439658	918		4938	5856
	13502468823	7335	110349	117684
 */

public class FlowProvince {
	static class FlowProMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] fields = value.toString().split("\t");
			String tel = fields[1];
			long upFlow = Long.parseLong(fields[fields.length-3]);
			long dowFlow = Long.parseLong(fields[fields.length-2]);
			
			context.write(new FlowBean(upFlow, dowFlow), new Text(tel));
		}
	}
	
	static class FlowProReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		@Override
		protected void reduce(FlowBean key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long sumUp = 0;
			long sumDown = 0;
			String tel = "";
			for(Text val: values) {
				sumUp += key.getUpFlow();
				sumDown += key.getDowFlow();
				tel = val.toString();
			}
			
			FlowBean resultBean = new FlowBean(sumUp, sumDown);
			context.write(new Text(tel), resultBean);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if(args.length!=2) {
			args = new String[2];
			args[0] = "hdfs://master:9000/flow/input/data.log";
			args[1] = "hdfs://master:9000/province/output";
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "province job");
		job.setJarByClass(FlowProvince.class);
		job.setMapperClass(FlowProMapper.class);
		job.setReducerClass(FlowProReducer.class);
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		job.setPartitionerClass(FlowPatitioner.class);
		job.setSortComparatorClass(FlowProComparator.class);
		//reduceTask数量不能小于分类的数量
		job.setNumReduceTasks(6);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
