package cn.kizzlepc.bigdata.nio;

public class TimeClient {

	public static void main(String[] args) {
		int port = 8080;
		if(args!=null && args.length>0) {
			try {
				port = Integer.valueOf(args[0]);
			}catch(NumberFormatException e) {
				//采用默认值
			}
		}
		
		TimeClientHandle timeClient = new TimeClientHandle("127.0.0.1", port);
		new Thread(timeClient, "TimeClient-001").start();;
	}

}
