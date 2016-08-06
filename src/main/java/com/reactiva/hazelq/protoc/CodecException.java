package com.reactiva.hazelq.protoc;

import java.text.ParseException;

public class CodecException extends ParseException {

	public static enum Type{IO_ERR, BEAN_ERR, PARSE_ERR, META_ERR}
	private Type type = Type.PARSE_ERR;
	/**
	 * 
	 * @param msg
	 * @param offset
	 */
	public CodecException(String msg, int offset) {
		super(msg, offset);
		
	}
	private String metaName;
	@Override
	public String getMessage()
	{
		return metaName + " :: " + super.getMessage();
		
	}
	/**
	 * 
	 * @param msg
	 * @param offset
	 * @param cause
	 */
	public CodecException(String msg, int offset, Throwable cause) {
		this(msg, offset);
		initCause(cause);
	}
	/**
	 * 
	 * @param msg
	 * @param offset
	 * @param cause
	 * @param type
	 */
	public CodecException(String msg, int offset, Throwable cause, Type type) {
		this(msg, offset, cause);
		this.type = type;
	}
	/**
	 * 
	 * @param offset
	 * @param cause
	 * @param type
	 */
	public CodecException(int offset, Throwable cause, Type type) {
		this("[At offset:"+offset+"] "+type, offset, cause, type);
	}
	/**
	 * 
	 * @param cause
	 * @param type
	 */
	public CodecException(Throwable cause, Type type) {
		this("[Offset not available] "+type, -1, cause, type);
	}
	
	public Type getType() {
		return type;
	}
	public void setType(Type type) {
		this.type = type;
	}

	public String getMetaName() {
		return metaName;
	}
	public void setMetaName(String metaName) {
		this.metaName = metaName;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 2658816934719219014L;

}
