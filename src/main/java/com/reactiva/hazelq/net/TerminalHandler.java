package com.reactiva.hazelq.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
@Sharable
public class TerminalHandler extends ChannelInboundHandlerAdapter {

	private static final Logger log = LoggerFactory.getLogger(TerminalHandler.class);
	private boolean closeOnFlush = false;
	@Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception { 
		try 
        {
			
			ctx.writeAndFlush(msg).addListener(new ChannelFutureListener() {
				
				@Override
				public void operationComplete(ChannelFuture future) throws Exception {
					if(future.isSuccess())
						log.debug("End processing "+future.channel().remoteAddress());
					
					if(closeOnFlush)
		        	{
		        		future.channel().close();
		        	}
					
				}
			});
        	
        } finally {
            ReferenceCountUtil.release(msg);
            
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { 
        log.warn("Closing client connection on "+ctx.channel().remoteAddress(), cause);
        Utils.closeOnIOErr(ctx.channel());
    }

	public boolean isCloseOnFlush() {
		return closeOnFlush;
	}

	public void setCloseOnFlush(boolean closeOnFlush) {
		this.closeOnFlush = closeOnFlush;
	}
}
