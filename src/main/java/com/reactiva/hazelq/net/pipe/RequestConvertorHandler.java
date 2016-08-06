package com.reactiva.hazelq.net.pipe;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactiva.hazelq.protoc.CodecException;
import com.reactiva.hazelq.protoc.impl.HQCodecWrapper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RequestConvertorHandler extends ByteToMessageDecoder {
	private static final Logger log = LoggerFactory.getLogger(RequestConvertorHandler.class);
	/**
	 * Pass all helper class instances, if reqd.
	 * This class is NOT in singleton scope.
	 */
	private HQCodecWrapper codec;
	/**
	 * 
	 * @param codecHdlr
	 * @param rh
	 * @param buffCodec
	 */
	public RequestConvertorHandler(HQCodecWrapper codecHdlr)
	{
		this.codec = codecHdlr;
	}
	
	
	
	protected Object readAsStreamed(ByteBuf in) throws CodecException
	{
		log.debug("Reading request bytes");
		byte[] b = new byte[in.readableBytes()];
    	in.readBytes(b);
    	log.debug("Transforming request to object");
    	Object o  = null;//= codec.decode(ITOCLogin.class, new DataInputStream(new ByteArrayInputStream(b)));
    	log.debug("Transformed request to object");
    	return o;
	}
	
	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		
		if (log.isDebugEnabled()) {
			int totalLen = in.getInt(0);
			log.debug("Begin request conversion for LLLL - "+totalLen);
		}
		try {
			out.add(readAsStreamed(in));
		} catch (CodecException e) {
			log.error("-- Codec error --", e);
			out.add(e.getMessage());
		}
    	

	}

}
