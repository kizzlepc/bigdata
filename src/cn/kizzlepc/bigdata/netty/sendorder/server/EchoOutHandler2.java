package cn.kizzlepc.bigdata.netty.sendorder.server;

import java.net.SocketAddress;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class EchoOutHandler2 extends ChannelOutboundHandlerAdapter {

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable arg1) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void bind(ChannelHandlerContext ctx, SocketAddress arg1, ChannelPromise arg2) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void close(ChannelHandlerContext ctx, ChannelPromise arg1) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void connect(ChannelHandlerContext ctx, SocketAddress arg1, SocketAddress arg2, ChannelPromise arg3)
			throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void deregister(ChannelHandlerContext ctx, ChannelPromise arg1) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void disconnect(ChannelHandlerContext ctx, ChannelPromise arg1) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void flush(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void read(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
		//通知下一个OutboundHandler
		System.out.println("out2");
		super.write(ctx, msg, promise);
	}

}
