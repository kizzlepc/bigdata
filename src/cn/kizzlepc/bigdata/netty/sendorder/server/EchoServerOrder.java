package cn.kizzlepc.bigdata.netty.sendorder.server;


import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class EchoServerOrder {
	private final int port;
	
	public EchoServerOrder(int port) {
		this.port = port;
	}
	
	public void start() throws Exception {
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
					// TODO Auto-generated method stub
					ch.pipeline().addLast(new EchoInHandler1());
					ch.pipeline().addLast(new EchoInHandler2());
					ch.pipeline().addLast(new EchoOutHandler1());
					ch.pipeline().addLast(new EchoOutHandler2());
				}
			});
			
			ChannelFuture channelFuture = serverBootstrap.bind().sync();
			System.out.println("server listining ......"+channelFuture.channel().localAddress());
			channelFuture.channel().closeFuture().sync();
		}finally {
			eventLoopGroup.shutdownGracefully().sync();
		}
	}
	

	public static void main(String[] args) throws Exception {
		new EchoServerOrder(40000).start();
	}
}
