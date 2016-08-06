package com.reactiva.hazelq.net;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.pool.ChannelHealthChecker;
import io.netty.util.concurrent.Future;

public class Utils {

	private Utils(){}
	public static final int NO_HOST_REACHABLE = 400;
	public static final int CONNECTION_RESET = 401;
	/**
	 * 
	 * @param ch
	 * @param reason
	 */
	public static void closeOnFlush(Channel ch, ByteBuf reason) {
        if (ch.isActive()) {
            ch.writeAndFlush(reason == null ? Unpooled.EMPTY_BUFFER: reason);
            ch.close().syncUninterruptibly();
        }
    }
	public static void closeOnNoHost(Channel ch) {
        closeOnFlush(ch, Unpooled.copyInt(NO_HOST_REACHABLE));
    }
	public static void closeOnIOErr(Channel ch) {
        closeOnFlush(ch, Unpooled.copyInt(CONNECTION_RESET));
    }
	


	/**
	 * Ping to check if a channel is reachable.
	 * @param ch
	 */
	public static boolean isChannelReachable(final Channel ch) throws IOException
	{
		
		try {
			return pingChannel(ch);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new InterruptedIOException();
		} catch (ExecutionException e) {
			throw new IOException(e.getCause());
		} catch (TimeoutException e) {
			throw new IOException(e);
		}
        
	}
	private static boolean pingChannel(final Channel ch) throws InterruptedException, ExecutionException, TimeoutException {
		Future<Boolean> f = ChannelHealthChecker.ACTIVE.isHealthy(ch);
		return isHealthy(f);
		
	}
	private static boolean isHealthy(Future<Boolean> f) throws InterruptedException, ExecutionException, TimeoutException {
		if(f.get(30, TimeUnit.SECONDS) && f.isSuccess())
		{
			if(f.getNow())
			{
				return true;
			}
		}
		return false;
		
	}

}
