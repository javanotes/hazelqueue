package com.reactiva.hazelq.protoc.impl;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Date;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.springframework.util.Assert;

import com.reactiva.hazelq.protoc.AbstractLengthBasedCodec;
import com.reactiva.hazelq.protoc.CodecException;
import com.reactiva.hazelq.protoc.CodecException.Type;
import com.reactiva.hazelq.protoc.FormatMeta;
import com.reactiva.hazelq.protoc.ProtocolMeta;
import com.reactiva.hazelq.protoc.StreamedLengthBasedCodec;
import com.reactiva.hazelq.protoc.dto.HQDataSecHeader;
import com.reactiva.hazelq.protoc.dto.HQInboundHeader;
import com.reactiva.hazelq.protoc.dto.HQOutboundHeader;
import com.reactiva.hazelq.protoc.dto.HQRequest;
import com.reactiva.hazelq.protoc.dto.HQResponse;
import com.reactiva.hazelq.protoc.dto.HQTrailer;
import com.reactiva.hazelq.utils.CommonUtil;
/**
 * Encode/Decode a pojo bean class according to ITOC protocol specs.
 * @refer 800-17.0-SPECS-1 FINAL, August, 2008
 * @author esutdal
 *
 */
public class StreamedHQCodec extends AbstractLengthBasedCodec implements StreamedLengthBasedCodec {
	final Charset charset;
	public StreamedHQCodec(Charset charset) {
		this.charset = charset;
	}
	/**
	 * UTF8 charset
	 */
	public StreamedHQCodec() {
		this(StandardCharsets.UTF_8);
	}
	private static void writeAsNumeric(FormatMeta f, Object o, DataOutputStream out, AtomicInteger count) throws IOException
	{
		switch(f.getLength())
		{
			case 1:
				out.writeByte((byte)o);
				count.addAndGet(f.getLength());
				break;
			case 2:
				out.writeShort((short) o);
				count.addAndGet(f.getLength());
				break;
			case 3:
				byte[] b3i = CommonUtil.intTo3Bytes((int) o);
				out.write(b3i);
				count.addAndGet(f.getLength());
				break;
			case 4:
				out.writeInt((int) o);
				count.addAndGet(f.getLength());
				break;
			case 8:
				out.writeLong((long) o);
				count.addAndGet(f.getLength());
				break;
				default:
					throw new IOException("Unexpected byte length "+f.getLength()+" for field "+f.getFieldName());
		}
	}
	private static void fillBytes(FormatMeta f, DataOutputStream out, AtomicInteger count) throws IOException
	{
		byte[] bytes = new byte[f.getLength()];
		Arrays.fill(bytes, (byte)0);
		out.write(bytes, 0, f.getLength());
		count.addAndGet(f.getLength());
	}
	
	private void writeAsObject(FormatMeta f, Object o, DataOutputStream out, AtomicInteger count) throws CodecException, IOException
	{
		if (o != null) {
			encode(o, out);
		}
		else
		{
			fillBytes(f, out, count);
		}
	}
	protected void writeBytesAndCount(FormatMeta f, Object o, DataOutputStream out, AtomicInteger count) throws IOException, CodecException
	{
		
		if(o == null)
		{
			fillBytes(f, out, count);
			return;
		}
		Object o1 = f.checkBounds(o, charset);
		byte[] bytes;
			
		switch(f.getAttr())
		{
		case APPHEADER:
			writeAsObject(f, o1, out, count);
			break;
		case BINARY:
			out.write((byte[]) o1, 0, f.getLength());
			count.addAndGet(f.getLength());
			break;
		case INHEADER:
			writeAsObject(f, o1, out, count);
			break;
		case NUMERIC:
			writeAsNumeric(f, o1, out, count);
			break;
		case OUTHEADER:
			writeAsObject(f, o1, out, count);
			break;
		case TEXT:
			bytes = o1.toString().getBytes(charset);
			out.write(bytes, 0, f.getLength());
			count.addAndGet(f.getLength());
			break;
		case TRAILER:
			writeAsObject(f, o1, out, count);
			break;
		default:
			fillBytes(f, out, count);
			break;
			
		
		}
	}
	
	private static Number readAsNumeric(FormatMeta f, DataInputStream in) throws IOException
	{
		switch(f.getLength())
		{
			case 1:
				return in.readByte();
			case 2:
				return in.readShort();
			case 3:
				byte[] b3i = readFully(in, 3);
				return CommonUtil.intFrom3Bytes(b3i);
			case 4:
				return in.readInt();
			case 8:
				return in.readLong();
				default:
					throw new IOException("Unexpected byte length "+f.getLength()+" for field "+f.getFieldName());
		}
	}
	
	@Override
	protected Object readBytes(FormatMeta f, DataInputStream in) throws IOException, CodecException
	{
		Object ret = null;
		byte[] bytes;
	
		
		switch(f.getAttr())
		{
		case APPHEADER:
			bytes = readFully(in, f.getLength());
			ret = decode(HQDataSecHeader.class, new DataInputStream(new ByteArrayInputStream(bytes)));
			break;
		case INHEADER:
			bytes = readFully(in, f.getLength());
			ret = decode(HQInboundHeader.class, new DataInputStream(new ByteArrayInputStream(bytes)));
			break;
		case NUMERIC:
			ret = readAsNumeric(f, in);
			break;
		case OUTHEADER:
			bytes = readFully(in, f.getLength());
			ret = decode(HQOutboundHeader.class, new DataInputStream(new ByteArrayInputStream(bytes)));
			break;
		case TEXT:
			bytes = readFully(in, f.getLength());
			ret = new String(bytes, charset);
			break;
		case TRAILER:
			bytes = readFully(in, f.getLength());
			ret = decode(HQTrailer.class, new DataInputStream(new ByteArrayInputStream(bytes)));
			break;
		default:
			bytes = readFully(in, f.getLength());
			break;
			
		
		}
		return ret;
	}
	
	private static byte[] readFully(DataInputStream in, int len) throws IOException
	{
		byte[] b = new byte[len];
		int read = 0, totalRead = 0;
		do {
			read = in.read(b, totalRead, (len - totalRead));
			if(read == -1)
				break;
			totalRead += read;
		} while (totalRead < len);
		
		Assert.isTrue(totalRead == len, "Expecting "+len+" bytes. Got "+totalRead);
		return b;
	}
	
	private void readBytesAndSet(Object p, FormatMeta f, DataInputStream in) throws ReflectiveOperationException, IOException, CodecException
	{
		Object o = readBytes(f, in);
		if(f.isDateFld())
		{
			o = toDate(f, o);
		}
		f.getSetter().invoke(p, o);
	}
	
	private <T> T read(Class<T> protoClassType, DataInputStream in, ProtocolMeta meta) throws ReflectiveOperationException, CodecException 
	{
		T tObj = null;
		try {
			tObj = protoClassType.newInstance();
		} catch (InstantiationException | IllegalAccessException e2) {
			throw e2;
		}
		
		for(Entry<Integer, FormatMeta> e : meta.getFormats().entrySet())
		{
			int off = e.getKey();
			FormatMeta f = e.getValue();
			
			try {
				readBytesAndSet(tObj, f, in);
			} catch (Exception e1) {
				throw new CodecException(off, e1, Type.IO_ERR);
			}
			
		}
		
		return tObj;
	}
	
	
	private <T> void write(FormatMeta f, T protoClass, DataOutputStream out, AtomicInteger count) throws ReflectiveOperationException, IOException, CodecException
	{
		Object o = f.getGetter().invoke(protoClass);
		if(o instanceof Date)
		{
			o = fromDate(f, (Date) o);
		}
		
		try {
			writeBytesAndCount(f, o, out, count);
		} catch (IllegalArgumentException e) {
			throw new ReflectiveOperationException(e);
		}

	}
	private static final Logger log = org.slf4j.LoggerFactory.getLogger(StreamedHQCodec.class);
	
	/* (non-Javadoc)
	 * @see com.smsnow.protocol.ICodec#encode(T, java.io.DataOutputStream)
	 */
	@Override
	public <T> void encode(T protoClass, DataOutputStream out) throws CodecException
	{
		Assert.notNull(protoClass, "Null instance");
		ProtocolMeta meta = getMeta(protoClass.getClass());
		if(protoClass instanceof HQRequest)
		{
			HQRequest req = (HQRequest) protoClass;
			if(req.getMVSInboundITOCHeader() == null)
			{
				throw new CodecException("MVSInboundITOCHeader not set", -1);
			}
			req.getMVSInboundITOCHeader().setTotalMessageLenLLLL(CommonUtil.intToBytes(meta.getSize()));
		}
		else if(protoClass instanceof HQResponse)
		{
			HQResponse req = (HQResponse) protoClass;
			if(req.getMVSOutboundITOCHeader() == null)
			{
				throw new CodecException("MVSOutboundITOCHeader not set", -1);
			}
			req.getMVSOutboundITOCHeader().setTotalMessageLenLLLL(CommonUtil.intToBytes(meta.getSize()));
		}
		AtomicInteger c = new AtomicInteger();
		encode(protoClass, meta, out);
		log.debug(protoClass.getClass()+" Bytes written:: "+c.get());
		
	}
		
	
	/* (non-Javadoc)
	 * @see com.smsnow.protocol.ICodec#decode(java.lang.Class, java.io.DataInputStream)
	 */
	@Override
	public <T> T decode(Class<T> protoClassType, DataInputStream in) throws CodecException 
	{
		ProtocolMeta meta = getMeta(protoClassType);
		return decode(protoClassType, meta, in);
	}
	
	@Override
	public <T> void encode(T protoClass, ProtocolMeta metaData, DataOutputStream out) throws CodecException {
		try {
			Assert.notNull(metaData);
			validate(metaData, protoClass.getClass());
		} catch (Exception e2) {
			throw new CodecException(e2, Type.META_ERR);
		}
		AtomicInteger count = new AtomicInteger();
		for(Entry<Integer, FormatMeta> e : metaData.getFormats().entrySet())
		{
			int off = e.getKey();
			FormatMeta f = e.getValue();
			
			try {
				write(f, protoClass, out, count);
			} catch (ReflectiveOperationException e1) {
				throw new CodecException(metaData.getName(), off, e1, Type.BEAN_ERR);
			} catch (IOException e1) {
				throw new CodecException(metaData.getName(), off, e1, Type.IO_ERR);
			}
		}
		
	}
	
	private void validate(ProtocolMeta metaData, Class<?> protoType) {
		metaData.validate();
		for(FormatMeta fm : metaData.getFormats().values())
		{
			fm.introspect(protoType);
		}
	}
	
	@Override
	public <T> T decode(Class<T> protoClassType, ProtocolMeta metaData, DataInputStream in) throws CodecException {
		try {
			try {
				Assert.notNull(metaData);
				validate(metaData, protoClassType);
			} catch (Exception e2) {
				throw new CodecException(e2, Type.META_ERR);
			}
			return read(protoClassType, in, metaData);
		} catch (CodecException e) {
			e.setMetaName(metaData.getName());
			throw e;
		} catch (ReflectiveOperationException e) {
			CodecException ce = new CodecException(e, Type.BEAN_ERR);
			ce.setMetaName(metaData.getName());
			throw ce;
		}
	}
	
	
}
