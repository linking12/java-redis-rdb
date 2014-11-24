package com.pajk.rdb;

import java.util.ArrayList;
import java.util.List;

public class IntSet {
	private final byte[] buff;

	public IntSet(byte[] intsetByte) {
		super();
		this.buff = intsetByte;
	}

	private long decodeEncoding() {
		byte[] encodingbyte = splitByte(buff, 0, 4);
		return ntohl(encodingbyte);
	}

	private long decodeLength() {
		byte[] lengthbyte = splitByte(buff, 4, 4);
		return ntohl(lengthbyte);
	}

	public List<String> docodeIntsetValue() {
		int encoding = Integer.valueOf(Long.valueOf(decodeEncoding())
				.toString());
		int length = Integer.valueOf(Long.valueOf(decodeLength()).toString());
		byte[] intsetvalue = new byte[buff.length - 8];
		System.arraycopy(buff, 8, intsetvalue, 0, intsetvalue.length);
		int index = 0;
		List<String> values = new ArrayList<String>();
		for (int i = 0; i < length; i++) {
			byte[] val = new byte[encoding];
			System.arraycopy(intsetvalue, index, val, 0, encoding);
			index = (i + 1) * encoding;
			long value = ntohl(val);
			values.add(Long.valueOf(value).toString());
		}
		return values;
	}

	private long ntohl(byte[] buf) {
		return ((buf[3] & 0x00ff) << 24) + ((buf[2] & 0x00ff) << 16)
				+ ((buf[1] & 0x00ff) << 8) + ((buf[0] & 0x00ff));
	}

	public static byte[] splitByte(byte[] buf, int start, int length) {
		byte[] tempbuff = new byte[length];
		int index = 0;
		for (int i = start; i < start + length; i++) {
			tempbuff[index] = buf[i];
			index++;
		}
		return tempbuff;
	}

}
