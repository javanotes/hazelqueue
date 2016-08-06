package com.reactiva.hazelq.protoc.gen;

public class JFormat {

	public int getOffset() {
		return offset;
	}
	public void setOffset(int offset) {
		this.offset = offset;
	}
	public int getLength() {
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
	public String getAttribute() {
		return attribute;
	}
	public void setAttribute(String attribute) {
		this.attribute = attribute;
	}
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public String getConstant() {
		return constant;
	}
	public void setConstant(String constant) {
		this.constant = constant;
	}
	public JFormat(int offset, int length, String attribute, String content) {
		super();
		this.offset = offset;
		this.length = length;
		this.attribute = attribute.toUpperCase();
		this.content = content;
	}
	int offset, length;
	String attribute,content,constant;
}
