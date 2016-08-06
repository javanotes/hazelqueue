package com.reactiva.hazelq.protoc.impl;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import org.springframework.util.Assert;

import com.reactiva.hazelq.protoc.BufferedLengthBasedCodec;
import com.reactiva.hazelq.protoc.CodecException;
import com.reactiva.hazelq.protoc.LengthBasedCodec;
import com.reactiva.hazelq.protoc.ProtocolMeta;
import com.reactiva.hazelq.protoc.StreamedLengthBasedCodec;
/**
 * A wrapper class to make use either of streamed or buffered transport, in a configurable manner.
 * <b>NOTE</b>: buffered transport is experimental however, and hence not recommended.
 * @author esutdal
 *
 */
public class HQCodecWrapper implements LengthBasedCodec,StreamedLengthBasedCodec,BufferedLengthBasedCodec {

	/**
	 * Validate the meta data of a protocol class instance. Would throw exception if not valid.
	 * @param protoInstance
	 * @throws CodecException
	 */
	public void validateMeta(Object protoInstance) throws CodecException
	{
		Assert.notNull(str, "Not using streamed codec");
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		encode(protoInstance, new DataOutputStream(out));
		protoInstance = decode(protoInstance.getClass(), new DataInputStream(new ByteArrayInputStream(out.toByteArray())));
	}
	private StreamedHQCodec str = null;
	private BufferedHQCodec buff = null;
	private LengthBasedCodec fl;
	/**
	 * 
	 * @param useByteBuf
	 */
	public HQCodecWrapper(Charset charset, boolean useByteBuf) {
		if(useByteBuf){
			buff = new BufferedHQCodec(charset);
			fl = buff;
		}
		else{
			str = new StreamedHQCodec(charset);
			fl = str;
		}
	}
	
	@Override
	public <T> ByteBuffer encode(T protoClass) throws CodecException {
		Assert.notNull(buff, "Not a buffered codec");
		return buff.encode(protoClass);
	}
	@Override
	public <T> T decode(Class<T> protoClassType, ByteBuffer in) throws CodecException {
		Assert.notNull(buff, "Not a buffered codec");
		return buff.decode(protoClassType, in);
	}
	@Override
	public <T> ByteBuffer encode(T protoClass, ProtocolMeta metaData) throws CodecException {
		Assert.notNull(buff, "Not a buffered codec");
		return buff.encode(protoClass, metaData);
	}
	@Override
	public <T> T decode(Class<T> protoClassType, ProtocolMeta metaData, ByteBuffer in) throws CodecException {
		Assert.notNull(buff, "Not a buffered codec");
		return buff.decode(protoClassType, metaData, in);
	}
	@Override
	public <T> void encode(T protoClass, DataOutputStream out) throws CodecException {
		Assert.notNull(str, "Not a streamed codec");
		str.encode(protoClass, out);
		
	}
	@Override
	public <T> T decode(Class<T> protoClassType, DataInputStream in) throws CodecException {
		Assert.notNull(str, "Not a streamed codec");
		return str.decode(protoClassType, in);
	}
	@Override
	public <T> void encode(T protoClass, ProtocolMeta metaData, DataOutputStream out) throws CodecException {
		Assert.notNull(str, "Not a streamed codec");
		str.encode(protoClass, metaData, out);
	}
	@Override
	public <T> T decode(Class<T> protoClassType, ProtocolMeta metaData, DataInputStream in) throws CodecException {
		Assert.notNull(str, "Not a streamed codec");
		return str.decode(protoClassType, metaData, in);
	}

	@Override
	public <T> int sizeof(Class<T> type) throws CodecException {
		return fl.sizeof(type);
	}

}
