package com.reactiva.hazelq.protoc;

import java.lang.reflect.Method;
import java.nio.charset.Charset;

import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.reactiva.hazelq.utils.CommonUtil;

public class FormatMeta {
	/**
	 * 
	 * @param offset
	 * @param length
	 * @param attr
	 */
	FormatMeta(int offset, int length, Attribute attr) {
		super();
		this.offset = offset;
		this.length = length;
		this.attr = attr;
	}
	private volatile boolean introspected = false;
	/**
	 * Use this constructor in dynamic usage.
	 * @param offset
	 * @param length
	 * @param attr
	 * @param fieldName
	 */
	public FormatMeta(int offset, int length, Attribute attr, String fieldName) {
		this(offset, length, attr);
		setFieldName(fieldName);
	}
	public String getFieldName() {
		return fieldName;
	}
	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	final int offset;
	private final int length;
	private final Attribute attr;
	private String constant;
	private String fieldName;
	private boolean isDateFld, isStrictSetter;
	String dateFormat;
	private Method getter;
	private Method setter;
	public void getter(Method m) {
		setGetter(m);
	}
	public void setter(Method m) {
		setSetter(m);
	}
	public String getConstant() {
		return constant;
	}
	public void setConstant(String constant) {
		this.constant = constant;
	}
	private Object checkBoundsByteArray(Object o)
	{
		byte[] n = null;
		try {
			n = (byte[]) o;
		} catch (Exception e) {
			throw new IllegalArgumentException("Field => "+getFieldName(),e);
		}
		return CommonUtil.padBytes(n, getLength(), (byte) '0');
	}
	private Object checkBoundsNumeric(Object o)
	{
		Number n = null;
		try {
			n = (Number) o;
		} catch (Exception e) {
			throw new IllegalArgumentException("Field => "+getFieldName(),e);
		}
		switch(getLength())
		{
			case 1:
				return n.byteValue();
			case 2:
				return n.shortValue();
			case 3:
				return n.intValue();
			case 4:
				return n.intValue();
			case 8:
				return n.longValue();
				default:
					throw new IllegalArgumentException("Unexpected number length "+getLength()+" for field "+getFieldName());
		}
	}
	
	
	private Object checkBoundsString(Object o, byte[] bytes, Charset charset, byte padChar)
	{
		if(isStrictSetter())
		{
			Assert.isTrue(bytes.length == getLength(), "Expecting length: "+getLength()+" found: "+bytes.length+" for field "+getFieldName());
		}
		else
		{
			if(bytes.length != getLength())
			{
				return new String(CommonUtil.padBytes(bytes, getLength(), padChar), charset);
			}
						
		}
		return o;
	}
	/**
	 * Checks the bound as per the meta definition. Throws exception if bounds do not match 
	 * and {@link #isStrictSetter()} is enabled. Pads String values with padChar
	 * @param o
	 * @param charset 
	 * @param padChar 
	 * @return
	 * @throws IllegalArgumentException 
	 */
	public Object checkBounds(Object o, Charset charset, byte padChar) throws IllegalArgumentException
	{
		if (o != null) {
			byte[] bytes;
			switch (getAttr()) {
			case NUMERIC:
				return checkBoundsNumeric(o);
			case BINARY:
				return checkBoundsByteArray(o);
			case UNDEF:
				return checkBoundsByteArray(o);
			case TEXT:
				bytes = o.toString().getBytes(charset);
				return checkBoundsString(o, bytes, charset, padChar);
			default:
				return o;

			}
		}
		return o;
	}
	/**
	 * Checks the bound as per the meta definition. Throws exception if bounds do not match 
	 * and {@link #isStrictSetter()} is enabled. Pads with '*' for String values.
	 * @param o
	 * @param charset
	 * @return
	 * @throws IllegalArgumentException
	 */
	public Object checkBounds(Object o, Charset charset) throws IllegalArgumentException
	{
		return checkBounds(o, charset, (byte) '*');
	}
	@Override
	public String toString() {
		return "FormatMeta [offset=" + offset + ", length=" + length + ", attr=" + attr + ", constant=" + constant
				+ ", fieldName=" + fieldName + "]";
	}
	public void introspect(Class<?> protoClassTyp, Class<?>...args) {
		if (!introspected) {
			introspected = true;
			Method m = ClassUtils.getMethodIfAvailable(protoClassTyp, AbstractLengthBasedCodec.getter(getFieldName()));
			Assert.notNull(m, getFieldName() + " Expecting a public getter");
			m.setAccessible(true);
			getter(m);
			
			if(args.length > 0)
			{
				m = ClassUtils.getMethodIfAvailable(protoClassTyp, AbstractLengthBasedCodec.setter(getFieldName()), args);
				Assert.notNull(m, getFieldName() + " Expecting a public setter");
				m.setAccessible(true);
				setter(m);
			}
			else
			{
				String setter = AbstractLengthBasedCodec.setter(getFieldName());
				for(Method m2 : protoClassTyp.getMethods())
				{
					if(m2.getName().equals(setter))
					{
						m = m2;
						break;
					}
				}
				Assert.notNull(m, getFieldName() + " Expecting a public setter");
				m.setAccessible(true);
				setter(m);
			}
			
		}
	}
	public int getLength() {
		return length;
	}
	public Attribute getAttr() {
		return attr;
	}
	public boolean isDateFld() {
		return isDateFld;
	}
	public void setDateFld(boolean isDateFld) {
		this.isDateFld = isDateFld;
	}
	public Method getSetter() {
		return setter;
	}
	public void setSetter(Method setter) {
		this.setter = setter;
	}
	public Method getGetter() {
		return getter;
	}
	public void setGetter(Method getter) {
		this.getter = getter;
	}
	public boolean isStrictSetter() {
		return isStrictSetter;
	}
	public void setStrictSetter(boolean isStrictSetter) {
		this.isStrictSetter = isStrictSetter;
	}
}
