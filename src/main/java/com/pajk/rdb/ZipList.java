package com.pajk.rdb;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class ZipList {
	public static final int ZIPLIST_PREV_ENTRY_LENGTH = 254;

	public static final int ZIPLIST_ENTRY_FLAG_6BITLEN = 0; // 6λ���ڼ���
	public static final int ZIPLIST_ENTRY_FLAG_14BITLEN = 1;
	public static final int ZIPLIST_ENTRY_FLAG_5BYTELEN = 2; // 5�ֽ����ڼ���
	public static final int ZIPLIST_ENTRY_FLAG_N2BYTEVLAUE = 12; // ����2�ֽڵ��޷�����������entryֵ
	public static final int ZIPLIST_ENTRY_FLAG_N4BYTEVLAUE = 13;
	public static final int ZIPLIST_ENTRY_FLAG_N8BYTEVLAUE = 14;
	public static final int ZIPLIST_ENTRY_FLAG_N3BYTEVLAUE = 0x00f0;
	public static final int ZIPLIST_ENTRY_FLAG_N1BYTEVLAUE = 0x00fe;

	private byte[] ziplistByte; // zip list����

	public ZipList(byte[] ziplistByte) {
		super();
		this.ziplistByte = ziplistByte;
	}

	public int decodeEntryCount() {
		@SuppressWarnings("unused")
		byte[] zlbytes = readByte(4);
		@SuppressWarnings("unused")
		byte[] tail_offset = readByte(4);
		byte[] num_entries = readByte(2);
		ByteBuffer.wrap(num_entries).order(ByteOrder.LITTLE_ENDIAN);
		int entryCount = bytesToInt(num_entries);
		return entryCount;
	}

	public int getEndByte() {
		byte[] zlbytes = readByte(1);
		return zlbytes[0] & 0xFF;
	}

	public Object decodeEntryValue() {
		int length;
		Object value = null;
		byte[] prev_lengthbyte = readByte(1);
		int pre_length = prev_lengthbyte[0] & 0xFF;
		if (pre_length == ZIPLIST_PREV_ENTRY_LENGTH) {
			prev_lengthbyte = readByte(4);
			pre_length = bytesToInt(prev_lengthbyte);
		}
		byte[] entry_headerbuff = readByte(1);
		if ((entry_headerbuff[0] & 0x00C0) >> 6 == ZIPLIST_ENTRY_FLAG_6BITLEN) {
			length = entry_headerbuff[0] & 0x003F;
			byte[] buff = readByte(length);
			try {
				value = new String(buff, "ASCII");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		} else if ((entry_headerbuff[0] & 0x00C0) >> 6 == ZIPLIST_ENTRY_FLAG_14BITLEN) {
			length = ((entry_headerbuff[0] & 0x003F) << 8)
					| (readByte(1)[0] & 0x00ff);
			byte[] buff = readByte(length);
			try {
				value = new String(buff, "ASCII");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		} else if ((entry_headerbuff[0] & 0x00C0) >> 6 == ZIPLIST_ENTRY_FLAG_5BYTELEN) {
			byte[] lengthbuff = readByte(4);
			ByteBuffer.wrap(lengthbuff).order(ByteOrder.BIG_ENDIAN);
			length = bytesToInt(lengthbuff);
			byte[] buff = readByte(length);
			try {
				value = new String(buff, "ASCII");
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
		} else if ((entry_headerbuff[0] & 0xff) >> 4 == ZIPLIST_ENTRY_FLAG_N2BYTEVLAUE) {
			length = 2;
			byte[] buff = readByte(length);// short
			value = bytesToShort(buff);
		} else if ((entry_headerbuff[0] & 0xff) >> 4 == ZIPLIST_ENTRY_FLAG_N4BYTEVLAUE) {
			length = 4;
			byte[] buff = readByte(length);// int
			value = bytesToInt(buff);
		} else if ((entry_headerbuff[0] & 0xff) >> 4 == ZIPLIST_ENTRY_FLAG_N8BYTEVLAUE) {
			length = 8;
			byte[] buff = readByte(length);// long
			value = bytesToLong(buff);
		} else if ((entry_headerbuff[0] & 0xff) == 240) {
			byte[] buff = readByte(3);
			byte[] newbuff = new byte[4];
			newbuff[0] = 0;
			System.arraycopy(buff, 0, newbuff, 1, buff.length);
			value = bytesToInt(newbuff) >> 8;
		} else if ((entry_headerbuff[0] & 0xff) == 254) {
			byte[] buff = readByte(1);
			value = (0x00ff & buff[0]);
		} else if ((entry_headerbuff[0] & 0xff) >= 241
				&& (entry_headerbuff[0] & 0xff) <= 253) {
			value = (entry_headerbuff[0] & 0xff) - 241;
		}
		return value;

	}

	private int bytesToInt(byte[] buff) {
		if (buff.length == 2)
			return ((buff[1] & 0x003F) << 8) | (buff[0] & 0x00ff);
		else if (buff.length == 4)
			return buff[3] & 0xFF | (buff[2] & 0xFF) << 8
					| (buff[1] & 0xFF) << 16 | (buff[0] & 0xFF) << 24;
		else
			return 0;
	}

	private short bytesToShort(byte[] buff) {
		return (short) (((buff[1] << 8) | buff[0] & 0xff));
	}

	private long bytesToLong(byte[] buff) {
		return ((buff[7] & 0x00ff) << 56) + ((buff[6] & 0x00ff) << 48)
				+ ((buff[5] & 0x00ff) << 40) + ((buff[4] & 0x00ff) << 32)
				+ ((buff[3] & 0x00ff) << 24) + ((buff[2] & 0x00ff) << 16)
				+ ((buff[1] & 0x00ff) << 8) + ((buff[0] & 0x00ff));
	}

	public byte[] readByte(int num) {
		byte[] buff = new byte[num];
		byte[] tempbuff = new byte[ziplistByte.length - num];
		for (int i = 0; i < num; i++) {
			buff[i] = ziplistByte[i];
		}
		System.arraycopy(ziplistByte, num, tempbuff, 0, tempbuff.length);
		ziplistByte = tempbuff;
		return buff;
	}

	public static void main(String[] args) {
		int t = (3 << 8);
		byte[] result = new byte[4];
		result[0] = (byte) (t >>> 24);// ȡ���8λ�ŵ�0�±�
		result[1] = (byte) (t >>> 16);// ȡ�θ�8Ϊ�ŵ�1�±�
		result[2] = (byte) (t >>> 8); // ȡ�ε�8λ�ŵ�2�±�
		result[3] = (byte) (t); // ȡ���8λ�ŵ�3�±�
		System.out.println(result);
	}

	public static void putInt(byte[] bb, int x, int index) {
		bb[index + 3] = (byte) (x >> 24);
		bb[index + 2] = (byte) (x >> 16);
		bb[index + 1] = (byte) (x >> 8);
		bb[index + 0] = (byte) (x >> 0);
	}
}
