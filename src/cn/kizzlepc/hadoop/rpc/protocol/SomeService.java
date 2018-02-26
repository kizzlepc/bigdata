package cn.kizzlepc.hadoop.rpc.protocol;

public interface SomeService {
	public long versionID = Long.MAX_VALUE;
	
	public String heartBeat(String name);
}
