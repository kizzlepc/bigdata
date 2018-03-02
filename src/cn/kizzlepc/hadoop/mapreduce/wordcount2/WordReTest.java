package cn.kizzlepc.hadoop.mapreduce.wordcount2;

public class WordReTest {
	private static String pattern = "[\\p{P}&&[^-_]]";
	
	public static void main(String[] args) {
		String str = "hello, world!Hdfs:Number of. bytes? written.";
		String new_str = str.replaceAll(pattern, " ");
		System.out.println(new_str);
	}

}
