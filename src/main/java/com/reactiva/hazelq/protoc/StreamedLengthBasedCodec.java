package com.reactiva.hazelq.protoc;

import java.io.DataInputStream;
import java.io.DataOutputStream;

public interface StreamedLengthBasedCodec extends LengthBasedCodec {

	/**
	 * Write a protocol instance to out stream.
	 * @param protoClass
	 * @param out
	 * @throws CodecException
	 */
	<T> void encode(T protoClass, DataOutputStream out) throws CodecException;

	

	/**
	 * Read an instance of protocol class from an in stream.
	 * @param <T>
	 * @param protoClassType
	 * @param in
	 * @throws CodecException
	 */
	<T> T decode(Class<T> protoClassType, DataInputStream in) throws CodecException;
	/**
	 * Write a protocol instance to out stream dynamically.
	 * @param protoClass
	 * @param metaData
	 * @param out
	 * @throws CodecException
	 */
	<T> void encode(T protoClass, ProtocolMeta metaData, DataOutputStream out) throws CodecException;
	
	/**
	 * Read an instance of protocol class from an in stream dynamically.
	 * @param protoClassType
	 * @param metaData
	 * @param in
	 * @return
	 * @throws CodecException
	 */
	<T> T decode(Class<T> protoClassType, ProtocolMeta metaData, DataInputStream in) throws CodecException;
}
