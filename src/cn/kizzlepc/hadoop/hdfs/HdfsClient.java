package cn.kizzlepc.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Before;
import org.junit.Test;

public class HdfsClient {
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		conf.set("dfs.replication", "3");
		//fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "hadoop");
	}
	
	
	/**
	 * 往hdfs上传本地文件(可以自动创建文件夹)
	 * @throws Exception
	 */
	@Test
	public void testAddFileToHdfs() throws Exception {
		Path src = new Path("E:/english.txt");
		Path dst = new Path("/wordcount/input/english.txt");
		fs.copyFromLocalFile(src, dst);
		fs.close();
	}
	
	
	/**
	 * 从hdfs中下载文件到本地
	 * @throws Exception
	 */
	@Test
	public void testCopyFileFromHdfs() throws Exception {
		Path src = new Path("/zookeeper-3.4.10.tar.gz");
		Path dst = new Path("D:/");
		fs.copyToLocalFile(src, dst);
		fs.close();
	}
	
	/**
	 * 创建文件夹
	 * @throws Exception 
	 */
	@Test
	public void testMkdir() throws Exception {
		Path newPath = new Path("/a1");
		fs.mkdirs(newPath);
		fs.close();
	}
	
	/**
	 * 删除文件夹（文件）
	 * @throws Exception 
	 */
	@Test
	public void testDeleteDir() throws Exception {
		Path delPath = new Path("/wordcount/input");
		//删除非空文件夹 true
		fs.delete(delPath, false);
		fs.close();
	}
	
	/**
	 * 重命名文件夹（文件）
	 * @throws IOException 
	 */
	@Test
	public void testRenameDir() throws IOException {
		Path oldName = new Path("/a1/a2");
		Path newName = new Path("/a1/newName");
		fs.rename(oldName, newName);
		fs.close();
	}
	
	/**
	 * 查看文件信息
	 * @throws Exception 
	 * @throws  
	 */
	@Test
	public void testListFiles() throws Exception {
		Path dst = new Path("/");
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(dst, true);
		while(listFiles.hasNext()) {
			LocatedFileStatus listFileStatus = listFiles.next();
			String fileName = listFileStatus.getPath().getName();
			System.out.println(fileName);
			short fileCount = listFileStatus.getReplication();
			System.out.println(fileCount);
			FsPermission fileUser = listFileStatus.getPermission();
			System.out.println(fileUser);
			long fileLength = listFileStatus.getLen();
			System.out.println(fileLength);
			long blockSize = listFileStatus.getBlockSize();
			System.out.println(blockSize);
			
			BlockLocation[] blockLocations = listFileStatus.getBlockLocations();
			for(BlockLocation bl:blockLocations) {
				String[] blockHosts = bl.getHosts();
				long blockLength = bl.getLength();
				long blockOffset = bl.getOffset();
				System.out.println(blockHosts+"-----"+blockLength+"-----"+blockOffset);
			}
		System.out.println("============================================================");
		}
		fs.close();
	}
	
	/**
	 * 查看文件和文件夹信息
	 * @throws Exception 
	 * @throws  
	 */
	@Test
	public void testListAll() throws Exception {
		Path dst = new Path("/wordcount");
		FileStatus[] listStatus = fs.listStatus(dst);
		for(FileStatus fs: listStatus) {
			String flag = "D--";
			if(fs.isFile())flag = "F-----";
			System.out.println(flag+fs.getPath().getName());
		}
	}
}




















