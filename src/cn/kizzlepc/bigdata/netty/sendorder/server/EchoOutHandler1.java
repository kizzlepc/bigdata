package cn.kizzlepc.bigdata.netty.sendorder.server;

import java.net.SocketAddress;
import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class EchoOutHandler1 extends ChannelOutboundHandlerAdapter {

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
		//向客户端发送消息
		System.out.println("out1");
		String currentTime = new Date(System.currentTimeMillis()).toString();
		ByteBuf resp = Unpooled.copiedBuffer(currentTime.getBytes());
		ctx.write(resp);
		ctx.flush();
	}

}
