package com.reactiva.hazelq.protoc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ReflectionUtils.FieldCallback;
import org.springframework.util.ReflectionUtils.FieldFilter;

import com.reactiva.hazelq.protoc.CodecException.Type;
/**
 * Abstract implementation of a {@linkplain LengthBasedCodec}.
 * @author esutdal
 *
 */
public abstract class AbstractLengthBasedCodec implements LengthBasedCodec {
	
	public <T> int sizeof(Class<T> type) throws CodecException
	{
		ProtocolMeta pm = getMeta(type);
		return pm.getSize();
	}
	/**
	 * Given a root package, search recursively for classes annotated with {@linkplain Protocol @Protocol}.
	 * @param basePkg
	 * @return
	 */
	public static Set<Class<?>> findProtocolClasses(String basePkg)
	  {
	    ClassPathScanningCandidateComponentProvider provider= new ClassPathScanningCandidateComponentProvider(false);
	    provider.addIncludeFilter(new TypeFilter() {
	      
	      @Override
	      public boolean match(MetadataReader metadataReader,
	          MetadataReaderFactory metadataReaderFactory) throws IOException {
	        AnnotationMetadata aMeta = metadataReader.getAnnotationMetadata();
	        return aMeta.hasAnnotation(Protocol.class.getName());
	      }
	    });
	    //consider the finder class to be in the root package
	    Set<BeanDefinition> beans = null;
	    try {
	      beans = provider.findCandidateComponents(basePkg);
	    } catch (Exception e) {
	      throw new BeanInitializationException("Unable to scan for protocol class", e);
	    }
	    
	    Set<Class<?>> classes = new HashSet<>();
	    if (beans != null && !beans.isEmpty()) {
	      classes = new HashSet<>(beans.size());
	      for (BeanDefinition bd : beans) {
	        try {
				classes.add(Class.forName(bd.getBeanClassName()));
			} catch (ClassNotFoundException e) {
				throw new BeanInitializationException("Unable to load protocol class", e);
			}
	      } 
	    }
	    
	    return classes;
	    
	    
	}
	public static Date toDate(FormatMeta f, Object o) throws ReflectiveOperationException
	{
		Date d = null;
		if(f.getAttr() == Attribute.TEXT)
		{
			SimpleDateFormat sdf = new SimpleDateFormat(f.dateFormat);
			try {
				d = sdf.parse(o.toString());
			} catch (ParseException e) {
				throw new ReflectiveOperationException(e);
			}
		}
		else if(f.getAttr() == Attribute.NUMERIC)
		{
			try {
				d = new Date((long) o);
			} catch (Exception e) {
				throw new ReflectiveOperationException(e);
			}
		}
		return d;
	}
	public static Object fromDate(FormatMeta f, Date d) throws ReflectiveOperationException, IOException
	{
		Object o = null;
		if(f.getAttr() == Attribute.TEXT)
		{
			SimpleDateFormat sdf = new SimpleDateFormat(f.dateFormat);
			o = sdf.format(d);
		}
		else if(f.getAttr() == Attribute.NUMERIC)
		{
			Long date = d.getTime();
			switch(f.getLength())
			{
				case 1:
					o = date.byteValue();
					break;
				case 2:
					o = date.shortValue();
					break;
				case 4:
					o = date.intValue();
					break;
				case 8:
					o = date;
					break;
					default:
						throw new IOException("Got length "+f.getLength()+" for attribute "+f.getAttr());
			}
		}
		return o;
	}
	public static String setter(String fName)
	{
		return "set" + (fName.charAt(0)+"").toUpperCase() + fName.substring(1);
	}
	public static String getter(String fName)
	{
		return "get" + (fName.charAt(0)+"").toUpperCase() + fName.substring(1);
	}
	private final Map<String, ProtocolMeta> cache = new HashMap<>();
	/**
	 * 
	 * @param protoClassType
	 * @return
	 */
	protected <T> ProtocolMeta getMeta(Class<T> protoClassType) throws CodecException
	{
		try {
			Assert.isTrue(protoClassType.isAnnotationPresent(Protocol.class), "Protocol classes should be annotated with @Protocol");
			if(!cache.containsKey(protoClassType.getName()))
			{
				synchronized (cache) {
					if (!cache.containsKey(protoClassType.getName())) {
						ProtocolMeta protoMeta = prepareMeta(protoClassType);
						protoMeta.validate();
						cache.put(protoClassType.getName(), protoMeta);
					}
				}
			}
		} catch (Exception e) {
			throw new CodecException(e, Type.META_ERR);
		}
		
		return cache.get(protoClassType.getName());
	}
	
	private ProtocolMeta prepareMeta(Class<?> protoClassTyp)
	{
		ProtocolMeta protoMeta = new ProtocolMeta(protoClassTyp.getAnnotation(Protocol.class).name());
		ReflectionUtils.doWithFields(protoClassTyp, new FieldCallback() {
			
			@Override
			public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException 
			{
				Format f = field.getAnnotation(Format.class);
				FormatMeta fm = new FormatMeta(f.offset(), f.length(), f.attribute());
				fm.setConstant(f.constant());
				fm.setFieldName(field.getName());
				fm.setDateFld(f.dateField());
				fm.dateFormat = f.dateFormat();
				fm.setStrictSetter(f.strictSetter());
				fm.introspect(protoClassTyp, field.getType());
								
				protoMeta.add(fm);
			}
		}, new FieldFilter() {
			
			@Override
			public boolean matches(Field field) {
				return field.isAnnotationPresent(Format.class);
			}
		});
		return protoMeta;
	}
	/**
	 * This is the class which does the write action. Change logic here to accommodate
	 * different protocol. with a counter.
	 * @param f
	 * @param o
	 * @param out
	 * @throws IOException
	 * @throws CodecException 
	 */
	protected void writeBytes(FormatMeta f, Object o, DataOutputStream out) throws IOException, CodecException
	{
		throw new IOException(new UnsupportedOperationException("Implementation to be provided by subclass"));
	}
	/**
	 * This is the class which does the read action. Change logic here to accommodate different
	 * protocol.
	 * @param f
	 * @param in
	 * @return
	 * @throws IOException
	 * @throws CodecException 
	 */
	protected Object readBytes(FormatMeta f, DataInputStream in) throws IOException, CodecException
	{
		throw new IOException(new UnsupportedOperationException("Implementation to be provided by subclass"));
	}
	/**
	 * This is the class which does the write action. Change logic here to accommodate
	 * different protocol.
	 * @deprecated
	 * @param f
	 * @param o
	 * @param out
	 * @throws IOException
	 * 
	 */
	protected void writeBytes(FormatMeta f, Object o, ByteBuffer out) throws IOException {
		throw new IOException(new UnsupportedOperationException("Implementation to be provided by subclass"));
	}
	/**
	 * This is the class which does the read action. Change logic here to accommodate different
	 * protocol.
	 * @deprecated
	 * @param f
	 * @param in
	 * @return
	 * @throws IOException
	 */
	protected Object readBytes(FormatMeta f, ByteBuffer in) throws IOException {
		throw new IOException(new UnsupportedOperationException("Implementation to be provided by subclass"));
	}
	
}
