package com.reactiva.hazelq.net.pipe;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactiva.hazelq.protoc.dto.HQRequest;

import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
@Sharable
public class RequestProcessorHandler extends MessageToMessageDecoder<HQRequest> {

	private static final Logger log = LoggerFactory.getLogger(RequestProcessorHandler.class);
	/**
	 * Pass all helper class instances, if reqd. Singleton scoped.
	 */
	public RequestProcessorHandler()
	{
		
	}
	
	/**
	 * Process the request using some service class.
	 * @param request
	 * @return
	 */
	private Serializable doProcess(Serializable request) throws Exception
	{
		log.info("[ECHO] Request processing .."+request);
		return request;
		
	}
	
	@Override
	protected void decode(ChannelHandlerContext ctx, HQRequest msg, List<Object> out) throws Exception {
		Serializable r = doProcess(msg);
		out.add(r);
		
	}

}
