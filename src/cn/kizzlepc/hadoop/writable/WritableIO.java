package cn.kizzlepc.hadoop.writable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.hadoop.io.IntWritable;

public class WritableIO {

	public static void main(String[] args) throws Exception {
		IntWritable writable = new IntWritable();
		writable.set(12345);
		
		//序列化
		byte[] bytes = serialize(writable);
		
		for(byte b:bytes) {
			System.out.println(b);
		}
		
		//反序列化
		IntWritable writable2 = new IntWritable();
		deserialize(bytes, writable2);
		System.out.println("xx:"+writable.get());
	}

	private static void deserialize(byte[] bytes, IntWritable writable2) throws Exception {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		DataInputStream dis = new DataInputStream(in);
		writable2.readFields(dis);
		dis.close();
	}

	private static byte[] serialize(IntWritable writable) throws Exception {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(out);
		writable.write(dos);
		dos.close();
		return out.toByteArray();
	}

}
