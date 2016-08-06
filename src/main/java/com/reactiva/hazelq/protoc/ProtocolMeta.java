package com.reactiva.hazelq.protoc;

import java.util.Map.Entry;
import java.util.TreeMap;

import org.springframework.util.Assert;

public class ProtocolMeta {

	private final TreeMap<Integer, FormatMeta> formats = new TreeMap<>();
	private final String name;
	public ProtocolMeta(String name)
	{
		this.name = name;
	}
	public void add(FormatMeta fm)
	{
		getFormats().put(fm.offset, fm);
	}
	
	@Override
	public String toString() {
		return name+" [formats=" + formats + "]";
	}
	private int size;
	public int getSize() {
		validate();
		return size;
	}
	private void setSize(int size) {
	}
	private volatile boolean validated = false;
	
	public void validate() {
		if (!validated) {
			synchronized (this) {
				if (!validated) {
					int off = 0, len = 0;
					setSize(len);
					for (Entry<Integer, FormatMeta> e : getFormats().entrySet()) {
						FormatMeta fm = e.getValue();
						Assert.isTrue(fm.offset == (off + len), getName() + " => Incorrect length specified at offset:" + off
								+". Expected "+(fm.offset - off)+", but found "+len);
						off = fm.offset;
						len = fm.getLength();
						
						size += len;
					}
					validated = true;
				}
			}
		}
		
	}
	public TreeMap<Integer, FormatMeta> getFormats() {
		return formats;
	}
	public String getName() {
		return name;
	}
}
