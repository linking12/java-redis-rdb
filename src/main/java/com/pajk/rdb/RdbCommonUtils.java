package com.pajk.rdb;

import static com.pajk.rdb.ByteUtils.readBytes;

import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;

/**
 * 
 * @author liushiming395
 * 
 */

public class RdbCommonUtils {

	public static final int REDIS_STRING = 0;
	public static final int REDIS_LIST = 1;
	public static final int REDIS_SET = 2;
	public static final int REDIS_ZSET = 3;
	public static final int REDIS_HASH = 4;
	public static final int REDIS_HASH_ZIPMAP = 9;
	public static final int REDIS_LIST_ZIPLIST = 10;
	public static final int REDIS_SET_INTSET = 11;
	public static final int REDIS_ZSET_ZIPLIST = 12;
	public static final int REDIS_HASH_ZIPLIST = 13;

	/*
	 * 整型数字的编码方式
	 */
	public static final int REDIS_RDB_ENC_INT8 = 0; /* 8 bit signed integer */
	public static final int REDIS_RDB_ENC_INT16 = 1; /* 16 bit signed integer */
	public static final int REDIS_RDB_ENC_INT32 = 2; /* 32 bit signed integer */
	public static final int REDIS_RDB_ENC_LZF = 3;
	/*
	 * 表示长度的方式
	 */
	public static final int REDIS_RDB_6BITLEN = 0;
	public static final int REDIS_RDB_14BITLEN = 1;
	public static final int REDIS_RDB_32BITLEN = 2;
	public static final int REDIS_RDB_ENCVAL = 3;

	public static final int REDIS_EXPIRETIME_FC = 252; /* 毫秒级过期时间,占用8个字节 */
	public static final int REDIS_EXPIRETIME_FD = 253; /* 秒级过期时间 ,占用8个字节 */
	public static final int REDIS_SELECTDB = 254; /* 数据库 (后面紧接着的就是数据库编号) */
	public static final int REDIS_EOF = 255; /* 结束符 */

	// ================common encoding method==========================//
	public static Object[] read_length_with_encoding(RandomAccessFile file) {
		long length = 0;
		boolean is_encoded = false;
		byte[] buf = new byte[2];
		if (!readBytes(file, buf, 0, 1))
			length = Long.MAX_VALUE;
		int type = (buf[0] & 0x00C0) >> 6;
		if (type == REDIS_RDB_6BITLEN) {
			length = buf[0] & 0x003F;
		} else if (type == REDIS_RDB_ENCVAL) {
			is_encoded = true;
			length = buf[0] & 0x003F;
		} else if (type == REDIS_RDB_14BITLEN) {
			if (!readBytes(file, buf, 1, 1))
				length = Long.MAX_VALUE;
			length = ((buf[0] & 0x003F) << 8) | (buf[1] & 0x00ff);
		} else {
			buf = new byte[4];
			if (!readBytes(file, buf, 0, 4))
				length = Long.MAX_VALUE;
			length = ((buf[3] & 0x00ff) << 24) + ((buf[2] & 0x00ff) << 16)
					+ ((buf[1] & 0x00ff) << 8) + ((buf[0] & 0x00ff));
		}
		return new Object[] { length, is_encoded };
	}

	public static String rdbLoadIntegerObject(RandomAccessFile file, int enctype) {
		byte[] enc = new byte[4];
		long val;
		if (enctype == REDIS_RDB_ENC_INT8) {
			if (!readBytes(file, enc, 0, 1))
				return null;
			val = (0x00ff & enc[0]);
		} else if (enctype == REDIS_RDB_ENC_INT16) {
			if (!readBytes(file, enc, 0, 2))
				return null;
			val = ((enc[1] & 0x00ff) << 8) | (enc[0] & 0x00ff);
		} else if (enctype == REDIS_RDB_ENC_INT32) {
			if (!readBytes(file, enc, 0, 4))
				return null;
			val = ((enc[3] & 0x00ff) << 24) + ((enc[2] & 0x00ff) << 16)
					+ ((enc[1] & 0x00ff) << 8) + ((enc[0] & 0x00ff));
		} else {
			return null;
		}
		return String.valueOf(val);
	}

	public static byte[] rdbLoadLzfStringObject(RandomAccessFile file) {
		long slen, clen;
		if ((clen = (Long) read_length_with_encoding(file)[0]) == Long.MAX_VALUE)
			return null;
		if ((slen = (Long) read_length_with_encoding(file)[0]) == Long.MAX_VALUE)
			return null;
		byte[] c = new byte[(int) clen];
		if (!readBytes(file, c, 0, (int) clen)) {
			return null;
		}
		byte[] s = new byte[(int) slen];
		LZFCompress.expand(c, 0, (int) clen, s, 0, (int) slen);
		return s;
	}

	public static Double rdbLoadDoubleValue(RandomAccessFile file) {
		byte[] buf = new byte[256];
		byte[] lenArray = new byte[1];
		Double val;
		if (!readBytes(file, lenArray, 0, 1))
			return null;
		int len = (0x00ff & lenArray[0]);
		switch (len) {
		case 255:
			val = Double.NEGATIVE_INFINITY;
			return val;
		case 254:
			val = Double.POSITIVE_INFINITY;
			return val;
		case 253:
			val = Double.NaN;
			return val;
		default:
			if (!readBytes(file, buf, 0, len)) {
				return null;
			}
			buf[len] = '\0';
			String str = "";
			try {
				str = new String(buf, 0, len, "ASCII");
			} catch (UnsupportedEncodingException e) {
				str = new String(buf, 0, len);
			}
			return Double.parseDouble(str);
		}
	}

	public static Object rdbLoadStringObject(RandomAccessFile file,
			boolean isByteOrObject) {
		Object[] lengthAndEncoded = read_length_with_encoding(file);
		long len = (Long) lengthAndEncoded[0];
		boolean encoded = (Boolean) lengthAndEncoded[1];
		if (encoded) {
			switch ((int) len) {
			case REDIS_RDB_ENC_INT8:
			case REDIS_RDB_ENC_INT16:
			case REDIS_RDB_ENC_INT32:
				return rdbLoadIntegerObject(file, (int) len);
			case REDIS_RDB_ENC_LZF:
				byte[] stringObjectByte = rdbLoadLzfStringObject(file);
				if (isByteOrObject) {
					try {
						return new String(stringObjectByte, "ASCII");
					} catch (UnsupportedEncodingException e) {
						return new String(stringObjectByte);
					}
				} else
					return stringObjectByte;
			default:
				return null;
			}
		} else {
			byte[] buf = new byte[(int) len];
			if (!readBytes(file, buf, 0, (int) len))
				return null;
			if (isByteOrObject) {
				try {
					return new String(buf, "ASCII");
				} catch (UnsupportedEncodingException e) {
					return new String(buf);
				}
			} else {
				return buf;
			}
		}
	}

	public static int rdbLoadType(RandomAccessFile file) {
		/* this byte needs to qualify as type */
		byte[] tt = new byte[1];
		if (readBytes(file, tt, 0, 1)) {
			int t = (0x00ff & tt[0]);
			if (t <= 4 || (t >= 9 && t <= 13) || t >= 252)
				return t;
		}
		return -1;
	}

	public static long rdbLoadTime(RandomAccessFile file, int type) {
		int timelen = (type == REDIS_EXPIRETIME_FC) ? 8 : 4;
		byte[] bb = new byte[8];
		if (readBytes(file, bb, 0, timelen)) {
			return ((((long) bb[0] & 0xff) << 56)
					| (((long) bb[1] & 0xff) << 48)
					| (((long) bb[2] & 0xff) << 40)
					| (((long) bb[3] & 0xff) << 32)
					| (((long) bb[4] & 0xff) << 24)
					| (((long) bb[5] & 0xff) << 16)
					| (((long) bb[6] & 0xff) << 8) | (((long) bb[7] & 0xff) << 0));
		}
		return -1;

	}

}
