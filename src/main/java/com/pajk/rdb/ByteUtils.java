package com.pajk.rdb;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

public class ByteUtils {

	// 字符转byte
	public static byte[] chars2bytes(String str) {
		try {
			return str.getBytes("ASCII");
		} catch (UnsupportedEncodingException e) {
			return str.getBytes();
		}
	}

	// byte转为long
	public static long strtol(byte[] buf, int start, int radix) {
		int len = start;
		for (; len < buf.length; len++) {
			if (buf[len] == 0x00)
				break;
		}
		char[] c = new char[len - start];
		for (int i = start; i < buf.length; i++) {
			c[i - start] = (char) (0x000000ff & buf[i]);
		}
		String str = new String(c);
		return Long.parseLong(str, radix);
	}

	// 把两个byte数组进行对比，判断是否相等
	public static int memcmp(byte[] buf1, int start1, byte[] buf2, int start2,
			int len) {
		int pos1 = start1;
		int pos2 = start2;
		for (int i = 0; i < len; i++) {
			pos1 = start1 + i;
			pos2 = start2 + i;
			if (buf1.length <= pos1 && buf2.length <= pos2)
				return 0;
			if (buf1.length <= pos1)
				return -1;
			if (buf2.length <= pos2)
				return 1;
			if (buf1[pos1] != buf2[pos2])
				return buf1[pos1] > buf2[pos2] ? 1 : -1;
		}
		return 0;
	}

	public static byte[] longToByte(long number) {
		long temp = number;
		byte[] b = new byte[8];
		for (int i = 0; i < b.length; i++) {
			b[i] = new Long(temp & 0xff).byteValue();// 将最低位保存在最低位
			temp = temp >> 8; // 向右移8位
		}
		return b;
	}

	static boolean readBytes(RandomAccessFile file, byte[] buf, int start,
			int num) {
		RandomAccessFile p = file;
		boolean peek = (num < 0) ? true : false;
		num = (num < 0) ? -num : num;
		try {
			if (p.getFilePointer() + num > p.length()) {
				return false;
			} else {
				p.readFully(buf, start, num);
				if (peek) {
					p.seek(p.getFilePointer() - num);
				}
			}
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	public static byte[] readByte(byte[] oldByte, int num) {
		byte[] buff = new byte[num];
		byte[] tempbuff = new byte[oldByte.length - num];
		for (int i = 0; i < num; i++) {
			buff[i] = oldByte[i];
		}
		System.arraycopy(oldByte, num, tempbuff, 0, tempbuff.length);
		oldByte = tempbuff;
		return buff;
	}

}
