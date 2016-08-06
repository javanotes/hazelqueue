package com.reactiva.hazelq.protoc;

public interface LengthBasedCodec {
	/**
	 * Byte length for a protocol class type.
	 * @param type
	 * @return
	 * @throws CodecException
	 */
	<T> int sizeof(Class<T> type) throws CodecException;

}