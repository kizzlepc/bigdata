package cn.kizzlepc.hadoop.rpc.client;

import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import cn.kizzlepc.hadoop.rpc.protocol.SomeService;

public class MyClient {

	public static void main(String[] args) throws Exception {
		SomeService someService = RPC.getProxy(SomeService.class, Long.MAX_VALUE , new InetSocketAddress("localhost", 5555), new Configuration());
		String res = someService.heartBeat("kizzlepc");
		System.out.println(res);
	}

}
