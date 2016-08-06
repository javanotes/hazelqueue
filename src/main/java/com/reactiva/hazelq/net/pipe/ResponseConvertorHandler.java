package com.reactiva.hazelq.net.pipe;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactiva.hazelq.protoc.CodecException;
import com.reactiva.hazelq.protoc.impl.HQCodecWrapper;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
@Sharable
public class ResponseConvertorHandler extends MessageToByteEncoder<Serializable> {

	private static final Logger log = LoggerFactory.getLogger(ResponseConvertorHandler.class);
	private HQCodecWrapper codec;
	private boolean buffCodec;
	/**
	 * 
	 * @param codecHdlr
	 * @param rh
	 * @param buffCodec
	 */
	public ResponseConvertorHandler(HQCodecWrapper codecHdlr, boolean buffCodec) {
		this.codec = codecHdlr;
		this.buffCodec = buffCodec;
	}
	/**
	 * Write the response to out stream.
	 * @param resp
	 * @param out
	 * @throws IOException 
	 * @throws CodecException 
	 */
	protected void write(Serializable resp, DataOutputStream out) throws CodecException
	{
		codec.encode(resp, out);
	}
	protected ByteBuffer write(Serializable resp) throws IOException, CodecException
	{
		return codec.encode(resp);
	}
	@Override
	protected void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out) throws Exception {
		if (buffCodec) {
			out.writeBytes(write(msg));
		}
		else
		{
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			log.debug("Writing response of type "+msg.getClass());
			try {
				write(msg, new DataOutputStream(bytes));
			} catch (CodecException e) {
				log.error("--Response conversion error--", e);
			}
			out.writeBytes(bytes.toByteArray());
			
		}
	}

}
