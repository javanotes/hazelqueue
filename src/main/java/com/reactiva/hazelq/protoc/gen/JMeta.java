package com.reactiva.hazelq.protoc.gen;

import java.util.HashMap;
import java.util.Map;

public class JMeta {

	public String getPackageName() {
		return packageName;
	}
	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}
	public String getClassName() {
		return className;
	}
	public void setClassName(String className) {
		this.className = className;
	}
	public Map<JField, JFormat> getFields() {
		return fields;
	}
	public void setFields(Map<JField, JFormat> fields) {
		this.fields = fields;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	String packageName = "", className, name = "";
	Map<JField, JFormat> fields = new HashMap<>();
	short typeCode;
	public void setTypeCode(short typeCode) {
		this.typeCode = typeCode;
	}
	public short getTypeCode() {
		return typeCode;
	}
	
	public JMeta(String className, short typeCode)
	{
		this.className = className;
		this.typeCode = typeCode;
	}
	public JMeta() {
	}
}
