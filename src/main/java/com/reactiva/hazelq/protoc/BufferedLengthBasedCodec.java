package com.reactiva.hazelq.protoc;

import java.nio.ByteBuffer;

public interface BufferedLengthBasedCodec extends LengthBasedCodec{

	/**
	 * Convert a protocol instance to byte buffer.
	 * @param protoClass
	 * @return
	 * @throws CodecException
	 */
	<T> ByteBuffer encode(T protoClass) throws CodecException;
	/**
	 * Read an instance of protocol class from input byte buffer.
	 * @param protoClassType
	 * @param in
	 * @return
	 * @throws CodecException
	 */
	<T> T decode(Class<T> protoClassType, ByteBuffer in) throws CodecException;
	/**
	 * Write a protocol instance to out stream dynamically.
	 * @param protoClass
	 * @param metaData
	 * @return
	 * @throws CodecException
	 */
	<T> ByteBuffer encode(T protoClass, ProtocolMeta metaData) throws CodecException;
	/**
	 * Read an instance of protocol class from an in stream dynamically.
	 * @param protoClassType
	 * @param metaData
	 * @param in
	 * @return
	 * @throws CodecException
	 */
	<T> T decode(Class<T> protoClassType, ProtocolMeta metaData, ByteBuffer in) throws CodecException;
}
