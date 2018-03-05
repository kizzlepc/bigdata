package cn.kizzlepc.hadoop.mapreduce.order;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.BeanUtilsBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
id		date		pid		amount
1001,	20150710,	P0001,	2

id		pname		category_id		price
P0001,	小米5	,		1000,			2
 */
public class OrderJoin {
	static class OrderMapper extends Mapper<LongWritable, Text, Text, InfoBean>{
		InfoBean bean = new InfoBean();
		String pid = "";
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			
			FileSplit fs = (FileSplit) context.getInputSplit();
			String fileName = fs.getPath().getName();
			if(fileName.contains("order")) {
				Integer id = Integer.parseInt(fields[0]);
				String dateString = fields[1];
				pid = fields[2];
				Integer amount = Integer.parseInt(fields[3]);			
				bean.set(id, dateString, pid, amount, "", 0, 0, "o");
			}else {
				pid = fields[0];
				String pname = fields[1];
				Integer category_id = Integer.parseInt(fields[2]);
				Long price = Long.parseLong(fields[3]);
				bean.set(0, "", pid, 0, pname, category_id, price, "p");
			}
			context.write(new Text(pid), bean);
		}
	}
	
	static class OrderReducer extends Reducer<Text, InfoBean, InfoBean, NullWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<InfoBean> values,Context context) throws IOException, InterruptedException {
			InfoBean pbean = new InfoBean();
			ArrayList<InfoBean> beans = new ArrayList<InfoBean>();
			
			for(InfoBean val: values) {
				String flag = val.getFlag();
				if("p".equals(flag)) {
					try {
						BeanUtils.copyProperties(pbean, val);
					} catch (IllegalAccessException | InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}else {
					try {
						InfoBean obean = new InfoBean();
						BeanUtils.copyProperties(obean, val);
						beans.add(obean);
					} catch (IllegalAccessException | InvocationTargetException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			for(InfoBean o: beans) {
				o.setPname(pbean.getPname());
				o.setCategory_id(pbean.getCategory_id());
				o.setPrice(pbean.getPrice());
				context.write(o, NullWritable.get());
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length!=2) {
			args = new String[2];
			args[0] = "hdfs://master:9000/order/input";
			args[1] = "hdfs://master:9000/order/output5";
		}
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(OrderJoin.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		job.setOutputKeyClass(InfoBean.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.out.println(job.waitForCompletion(true)?0:1);
	}

}
