package com.reactiva.hazelq.protoc.gen;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;
import java.util.Scanner;

import org.springframework.util.StringUtils;

import com.reactiva.hazelq.protoc.Attribute;
import com.reactiva.hazelq.protoc.dto.HQDataSecHeader;
import com.reactiva.hazelq.protoc.dto.HQInboundHeader;
import com.reactiva.hazelq.protoc.dto.HQOutboundHeader;
import com.reactiva.hazelq.protoc.dto.HQTrailer;

public class FormatParser {

	private static JFormat parseLine(String nextLine)
	{
		int off = 0, len = 0;
		String attr = "", name = "", cons = "";
		try (Scanner l = new Scanner(nextLine)) {

			if (l.hasNext())
				off = l.nextInt();

			if (l.hasNext())
				len = l.nextInt();

			if (l.hasNext())
				attr = l.next();

			if (l.hasNext())
				name = l.next();

			if (l.hasNext())
				cons = l.next();

		}
		

		JFormat fmt = new JFormat(off, len, attr, name);
		fmt.setConstant(cons);
		
		return fmt;

	}
	
	private static JField numeric(JFormat f)
	{
		switch(f.length)
		{
		case 1:
			return new JField(f.getContent(), Byte.TYPE);
		case 2:
			return new JField(f.getContent(), Short.TYPE);
		case 3:
			return new JField(f.getContent(), Integer.TYPE);
		case 4:
			return new JField(f.getContent(), Integer.TYPE);
		case 8:
			return new JField(f.getContent(), Long.TYPE);
		default:
			throw new IllegalArgumentException(f.content+" Invalid numeric length " + f.length);
		}
	}
	private static JField validate(JFormat f)
	{
		if(StringUtils.hasText(f.getContent()))
		{
			String attr = f.getAttribute().toUpperCase();
			if(Attribute.TEXT.name().equals(attr))
			{
				return new JField(f.getContent(), String.class);
			}
			else if(Attribute.NUMERIC.name().equals(attr))
			{
				return numeric(f);
				
			}
			/*else if(Attribute.BINARY.name().equals(attr))
			{
				return numeric(f);
			}*/
			else if(Attribute.APPHEADER.name().equals(attr))
			{
				return new JField(f.getContent(), HQDataSecHeader.class);
			}
			else if(Attribute.INHEADER.name().equals(attr))
			{
				return new JField(f.getContent(), HQInboundHeader.class);
			}
			else if(Attribute.OUTHEADER.name().equals(attr))
			{
				return new JField(f.getContent(), HQOutboundHeader.class);
			}
			else if(Attribute.TRAILER.name().equals(attr))
			{
				return new JField(f.getContent(), HQTrailer.class);
			}
			else
			{
				return new JField(f.getContent(), byte[].class);
			}
		}
		return null;
	}
	/**
	 * Parse a source file and return metafiles
	 * @param source
	 * @param err
	 * @return
	 */
	public static JMeta parse(Readable source, List<Throwable> err)
	{
		JMeta meta = new JMeta();
		try(Scanner s = new Scanner(source))
		{
			JFormat fmt;
			JField fld;
			String line;
			while (s.hasNextLine()) 
			{
				try 
				{
					line = s.nextLine();
					fmt = parseLine(line);
					fld = validate(fmt);
					if(fld == null)
					{
						throw new Exception("Field name not defined in line ["+line+"]");
					}
					meta.getFields().put(fld, fmt);
				} catch (Exception e) {
					if(err != null)
						err.add(e);
				}

			}
		}
		catch(Exception e)
		{
			if(err != null)
				err.add(e);
		}
		
		return meta;
	}
	/**
	 * 
	 * @param templateFile
	 * @param err
	 * @return
	 * @throws FileNotFoundException
	 */
	public static JMeta parse(String templateFile, List<Throwable> err) throws FileNotFoundException {
		return parse(new FileReader(templateFile), err);
	}
}
