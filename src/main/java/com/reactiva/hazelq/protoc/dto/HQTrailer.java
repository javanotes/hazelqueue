/* Generated by SMSNOW protocol CodeGen on Wed Aug 03 16:16:16 IST 2016. 

0 2 Binary trailerLenLL 4
2 2 Binary ZZ 0
*/
package com.reactiva.hazelq.protoc.dto;

import java.io.Serializable;

import com.reactiva.hazelq.protoc.Attribute;
import com.reactiva.hazelq.protoc.Format;
import com.reactiva.hazelq.protoc.Protocol;

@Protocol(name = "ITOCTRAILER")
public class HQTrailer implements Serializable {

	private static final long serialVersionUID = 1L;
	@Format(attribute = Attribute.BINARY, offset = 0, length = 2, constant = "4")
	private byte[] trailerLenLL = new byte[2];
	@Format(attribute = Attribute.BINARY, offset = 2, length = 2, constant = "0")
	private byte[] zZ = new byte[2];

	public byte[] getTrailerLenLL() {
		return trailerLenLL;
	}

	public void setTrailerLenLL(byte[] trailerLenLL) {
		this.trailerLenLL = trailerLenLL;
	}

	public byte[] getZZ() {
		return zZ;
	}

	public void setZZ(byte[] zZ) {
		this.zZ = zZ;
	}

	public HQTrailer() {
	}
}