package cn.kizzlepc.bigdata.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

public class MultiplexerTimeServer implements Runnable {
	private Selector selector;
	private ServerSocketChannel serverChannel;
	private volatile boolean stop;
	
	/**
	 * 初始化多路复用器、绑定监听端口
	 * @param port
	 */
	public MultiplexerTimeServer(int port) {
		try {
			selector = Selector.open();
			serverChannel = ServerSocketChannel.open();
			serverChannel.configureBlocking(false);
			serverChannel.socket().bind(new InetSocketAddress(port), 1024);
			serverChannel.register(selector, SelectionKey.OP_ACCEPT);
			System.out.println("The time server is start in port:"+port);
		}catch(IOException e){
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	@Override
	public void run() {
		while(!stop) {
			try {
				//等待某个通道的超时时间
				selector.select(1000);
				Set<SelectionKey> selectionKeys = selector.selectedKeys();
				Iterator<SelectionKey> it = selectionKeys.iterator();
				SelectionKey key =  null;
				while(it.hasNext()) {
					key = it.next();
					it.remove();
					try{
						handleInput(key);
					}catch(Exception e) {
						if(key!=null) {
							key.cancel();
							if(key.channel()!=null) {
								key.channel().close();
							}
						}
					}
				}
			}catch(Throwable t) {
				t.printStackTrace();
			}
		}
		
		//多路复用器关闭后，所有注册在上面的channel和pipe等资源都会自动去注册并关闭，所以不需要重复释放资源
		if(selector!=null) {
			try{
				selector.close();
			}catch(IOException e) {
				e.printStackTrace();
			}
		}
	}

	private void handleInput(SelectionKey key) throws IOException {
		//此键是否有效
		if(key.isValid()) {
			//此键是否准备好接受新的套接字连接
			if(key.isAcceptable()) {
				ServerSocketChannel ssc = (ServerSocketChannel)key.channel();
				SocketChannel sc = ssc.accept();
				sc.configureBlocking(false);
				sc.register(selector, SelectionKey.OP_READ);
			}
			//此键是否准备好进行读取
			if(key.isReadable()) {
				SocketChannel sc = (SocketChannel)key.channel();
				//分配新的字节缓存区
				ByteBuffer readBuffer = ByteBuffer.allocate(1024);
				int readBytes = sc.read(readBuffer);
				if(readBytes>0) {
					//为写入做好准备
					readBuffer.flip();
					//此缓存区中的剩余元素数
					byte[] bytes = new byte[readBuffer.remaining()];
					readBuffer.get(bytes);
					String body = new String(bytes, "UTF-8");
					System.out.println("The time server receive order:"+body);	
					String currentTime = "Query time order".equalsIgnoreCase(body)
							?new java.util.Date(System.currentTimeMillis()).toString()
							:"Bad order";
					doWrite(sc, currentTime);
				}else if(readBytes<0){
					//对端链路关闭
					key.channel();
					sc.close();
				}else {
					//读到0字节，忽略
				};
			}
		}
	}

	private void doWrite(SocketChannel channel, String response) throws IOException {
		if(response!=null && response.length()>0) {
			byte[] bytes = response.getBytes();
			ByteBuffer writeBuffer = ByteBuffer.allocate(bytes.length);
			writeBuffer.put(bytes);
			writeBuffer.flip();
			channel.write(writeBuffer);
		}
	}

}
	
	
	
	
	
	
	
	
	
	
	
