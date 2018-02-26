package cn.kizzlepc.bigdata.netty.sendstring.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class EchoServer {

	private final int port;
	
	public EchoServer(int port) {
		this.port = port;
	}
	
	public void start() throws Exception{
		EventLoopGroup eventLoopGroup = null;
		try {
			ServerBootstrap serverBootstrap = new ServerBootstrap();
			eventLoopGroup = new NioEventLoopGroup();
			serverBootstrap.group(eventLoopGroup)
			.channel(NioServerSocketChannel.class)
			.localAddress("localhost", port)
			.childHandler(new ChannelInitializer<Channel>() {

				@Override
				protected void initChannel(Channel ch) throws Exception {
					ch.pipeline().addLast(new EchoServerHandler());
				}
			});
			
			ChannelFuture channelFuture = serverBootstrap.bind().sync();
			System.out.println("start listening ......, port is "+channelFuture.channel().localAddress());
			channelFuture.channel().closeFuture().sync();
			
		}finally {
			eventLoopGroup.shutdownGracefully().sync();
		}
	}
	
	public static void main(String[] args) throws Exception {
		new EchoServer(20000).start();
	}
}
