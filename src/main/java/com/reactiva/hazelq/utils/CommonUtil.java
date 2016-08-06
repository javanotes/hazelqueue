package com.reactiva.hazelq.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import org.springframework.util.Assert;

public class CommonUtil {

	public static void main(String[] args) {
		int n = 1;
		byte[] b = intTo3Bytes(n);
		int o = intFrom3Bytes(b);
		System.out.println("In "+n +" Out "+o);
		
		System.out.println((byte)0);
	}
	public static byte[] intToBytes(int i)
	{
		return ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(i).array();
	}
	public static byte[] intTo3Bytes(int i)
	{
		Assert.isTrue(i >= 0 && i <= 255, "3byte num range [0, 255] both inclusive. Found "+i);
		byte[] b = new byte[3];
		
		if (i > 0) {
			b[0] = (byte) (i & 0xFF);
			b[1] = (byte) ((i >> 8) & 0xFF);
			b[2] = (byte) ((i >> 16) & 0xFF);
		}
		else
		{
			Arrays.fill(b, (byte)0);
		}
		return b;
	}
	public static int intFrom3Bytes(byte[] b)
	{
		Assert.notNull(b);
		Assert.isTrue(b.length >= 3, "Less than 3 bytes");
		
		return (b[0] & 0xFF) | ((b[1] << 8) & 0xFF) | ((b[2] << 16) & 0xFF);
	}
	public static byte[] padBytes(byte[] origBytes, int targetLen, byte padByte)
	{
		int len = origBytes.length;
		byte[] targetBytes = Arrays.copyOf(origBytes, targetLen);
		if(targetLen > len)
		{
			for(int i=len; i<targetLen; i++)
			{
				targetBytes[i] = padByte;
			}
		}
		
		return targetBytes;
	}
}
