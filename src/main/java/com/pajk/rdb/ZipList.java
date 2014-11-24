package com.pajk.rdb;

import java.io.UnsupportedEncodingException;

public class ZipList {
	public static final int ZIPLIST_PREV_ENTRY_LENGTH = 254;
	public static final int ZIPLIST_END = 255; // zip list������

	public static final int ZIPLIST_ENTRY_FLAG_6BITLEN = 0; // 6λ���ڼ���
	public static final int ZIPLIST_ENTRY_FLAG_14BITLEN = 1;
	public static final int ZIPLIST_ENTRY_FLAG_5BYTELEN = 2; // 5�ֽ����ڼ���
	public static final int ZIPLIST_ENTRY_FLAG_N2BYTEVLAUE = 12; // ����2�ֽڵ��޷�����������entryֵ
	public static final int ZIPLIST_ENTRY_FLAG_N4BYTEVLAUE = 13;
	public static final int ZIPLIST_ENTRY_FLAG_N8BYTEVLAUE = 14;
	public static final int ZIPLIST_ENTRY_FLAG_N3BYTEVLAUE = 0x00f0;
	public static final int ZIPLIST_ENTRY_FLAG_N1BYTEVLAUE = 0x00fe;

	private final byte[] ziplistByte; // zip list����
	private int index; // byte�����±�

	public ZipList(byte[] ziplistByte) {
		super();
		this.ziplistByte = ziplistByte;
		this.index = 8;
	}

	public int getEndByte() {
		return ziplistByte[index];
	}

	/*
	 * ռ1��5���ֽ�,��ʾǰһ��entry���ֽڳ��ȣ���һ��entry��0
	 * �����һ���ֽ�����ֵ����254,������4���ֽڱ�ʾ����,�����һ���ֽ�����ֵ���ǳ���
	 */
	private void decodePrevEntryFlag() {
		int len = prevEntryIntegerValue();
		if (len < ZIPLIST_PREV_ENTRY_LENGTH) {
			index++;
		} else {
			index = index + 5;
		}
	}

	/*
	 * ǰһ��entryռ�õ��ֽ���
	 */
	private int prevEntryIntegerValue() {
		int len = ziplistByte[index];
		if (len < ZIPLIST_PREV_ENTRY_LENGTH)
			return len;
		len = ((ziplistByte[index + 4] & 0x00ff) << 24)
				+ ((ziplistByte[index + 3] & 0x00ff) << 16)
				+ ((ziplistByte[index + 2] & 0x00ff) << 8)
				+ ((ziplistByte[index + 1] & 0x00ff));
		return len;
	}

	public int decodeEntryCount() {
		/*
		 * ռ2���ֽ�,entry�ĸ���,key��value����һ��entry,���Խ���Map��forѭ������Ҫ����2
		 */
		int entryCount = (((ziplistByte[index + 1] & 0x003F) << 8) | (ziplistByte[index] & 0x00ff));
		// ������2���ֽڣ��±��ƶ�2��λ��
		index = index + 2;
		return entryCount;
	}

	/**
	 * entry��value
	 * 
	 * @return
	 */
	public String decodeEntryValue() {
		decodePrevEntryFlag();

		Integer[] object = decodeEntrySpecialFlag();
		int entryDataByteLen = object[0]; // entry���ݵ��ֽڳ���

		String value;
		if (object[1] != null) {
			value = String.valueOf(object[1]);
		} else {
			value = byteToString(subbyte(ziplistByte, index, entryDataByteLen));
		}
		index = index + entryDataByteLen;
		return value;
	}

	/**
	 * special flag,ռ���ֽ���1��9֮��, ���ڱ�ʾentry����ռ���ֽڳ��Ȼ�entry������ֵ
	 * 
	 * @param value
	 * @param index
	 * @return Integer[] [entry data bytes length , entry integer value]
	 */
	private Integer[] decodeEntrySpecialFlag() {
		byte[] buf = new byte[9];
		int flagLen = 1;
		int entryLen = 0;
		Integer intValue = null;

		for (int i = 0; i < buf.length && ((index + i) < ziplistByte.length); i++) {
			buf[i] = ziplistByte[index + i];
		}
		int type = (buf[0] & 0x00C0) >> 6;
		if (buf[0] == ZIPLIST_ENTRY_FLAG_N1BYTEVLAUE) {
			/* Read next 1 byte integer value */
			flagLen = 1;
			entryLen = 1;
			intValue = (0x00ff & buf[1]);
		} else if (buf[0] == ZIPLIST_ENTRY_FLAG_N3BYTEVLAUE) {
			/* Read next 3 byte integer value */
			flagLen = 1;
			entryLen = 3;
			intValue = ((buf[3] & 0x00ff) << 16) + ((buf[2] & 0x00ff) << 8)
					+ ((buf[1] & 0x00ff));
		} else if (type == ZIPLIST_ENTRY_FLAG_6BITLEN) {
			/* Read a 6 bit len */
			flagLen = 1;
			entryLen = (buf[0] & 0x003F);
		} else if (type == ZIPLIST_ENTRY_FLAG_14BITLEN) {
			/* Read a 14 bit len */
			flagLen = 2;
			entryLen = ((buf[0] & 0x003F) << 8) | (buf[1] & 0x00ff);
		} else if (type == ZIPLIST_ENTRY_FLAG_5BYTELEN) {
			/* Read a 5 byte len */
			flagLen = 5;
			entryLen = ((buf[4] & 0x00ff) << 32) + ((buf[3] & 0x00ff) << 24)
					+ ((buf[2] & 0x00ff) << 16) + ((buf[1] & 0x00ff) << 8)
					+ ((buf[0] & 0x003F));
		} else {
			type = (buf[0] & 0x00f0) >> 4;
			if (type == ZIPLIST_ENTRY_FLAG_N2BYTEVLAUE) {
				/* Read next 2 byte integer value */
				flagLen = 1;
				entryLen = 2;
				intValue = ((buf[2] & 0x00ff) << 8) + ((buf[1] & 0x00ff));
			} else if (type == ZIPLIST_ENTRY_FLAG_N4BYTEVLAUE) {
				/* Read next 4 byte integer value */
				flagLen = 1;
				entryLen = 4;
				intValue = ((buf[4] & 0x00ff) << 24)
						+ ((buf[3] & 0x00ff) << 16) + ((buf[2] & 0x00ff) << 8)
						+ ((buf[1] & 0x00ff));
			} else if (type == ZIPLIST_ENTRY_FLAG_N8BYTEVLAUE) {
				/* Read next 8 byte integer value */
				flagLen = 1;
				entryLen = 8;
				intValue = ((buf[8] & 0x00ff) << 56)
						+ ((buf[7] & 0x00ff) << 48) + ((buf[6] & 0x00ff) << 40)
						+ ((buf[5] & 0x00ff) << 32) + ((buf[4] & 0x00ff) << 24)
						+ ((buf[3] & 0x00ff) << 16) + ((buf[2] & 0x00ff) << 8)
						+ ((buf[1] & 0x00ff));
			} else if (type == 15) {
				/* Read next 8 byte integer value */
				flagLen = 1;
				entryLen = (buf[0] & 0x000f);
			} else {
				ERROR(" Unknown entry special flag encoding (0x%02x) ", type);
			}
		}
		if (entryLen == 0) {
			ERROR(" entryLen is not 0 (0x%02x) ", entryLen);
		}
		// ������special flag��, index������
		index = index + flagLen;
		return new Integer[] { entryLen, intValue };
	}

	static byte[] subbyte(byte[] buf, int start, int len) {
		byte[] value = new byte[len];
		for (int i = 0; i < len; start++, i++) {
			value[i] = buf[start];
		}
		return value;
	}

	static String byteToString(byte[] buf) {
		try {
			return new String(buf, "ASCII");
		} catch (UnsupportedEncodingException e) {
			return new String(buf);
		}
	}

	static void ERROR(String msg, Object... args) {
		throw new RuntimeException(String.format(msg, args));
	}
}
