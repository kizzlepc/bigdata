package cn.kizzlepc.bigdata.netty.sendorder.server;

import java.util.Date;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class EchoInHandler2 extends ChannelInboundHandlerAdapter {

	@Override
	public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		//接收客户端的数据
		System.out.println("in2");
		ByteBuf readBuf = (ByteBuf)msg;
		byte[] req = new byte[readBuf.readableBytes()];
		readBuf.readBytes(req);
		String body = new String(req, "UTF-8");
		System.out.println("server recvfrom client data:"+body);
		//向客服端写数据
		System.out.println("server send data to client ......");
		String currentTime = new Date(System.currentTimeMillis()).toString();
		ByteBuf resp = Unpooled.copiedBuffer(currentTime.getBytes());
		ctx.write(resp);
	}

	@Override
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
		ctx.flush();
	}

	@Override
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
		// TODO Auto-generated method stub

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		cause.printStackTrace();
		ctx.close();
	}

	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object arg1) throws Exception {
		// TODO Auto-generated method stub

	}

}
