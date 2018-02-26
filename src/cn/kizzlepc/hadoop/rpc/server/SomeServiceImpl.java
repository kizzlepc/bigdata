package cn.kizzlepc.hadoop.rpc.server;

import cn.kizzlepc.hadoop.rpc.protocol.SomeService;

public class SomeServiceImpl implements SomeService {

	@Override
	public String heartBeat(String name) {
		System.out.println("Recvfrom client message:"+name+" heartbeat, connect success......");
		return "heartbeat success!";
	}

}
