package com.pajk.rdb;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class ZipMap {
	public static final int ZIPMAP_BIGLEN = 254; // zipMap��ʼ��
	public static final int ZIPMAP_END = 255; // zipMap������
	public static final int ZIPMAP_VALUE_MAX_FREE = 4;

	/*
	 * ռ1��5���ֽ� �����һ���ֽ�����ֵ����254,������4���ֽڱ�ʾ����,�����һ���ֽ�����ֵ���ǳ���
	 */
	static int decodeFlagLen(int _l) {
		if (_l < ZIPMAP_BIGLEN) {
			return 1;
		} else {
			return 5;
		}
	}

	/* ����һ���յ�zipMap */
	static ByteBuffer zipmapNew() {
		byte[] zm = new byte[2];
		zm[0] = 0; /* Length */
		zm[1] = (byte) ZIPMAP_END;
		return ByteBuffer.wrap(zm);
	}

	/*
	 * zipMapռ�õ��ֽ���
	 */
	static int zipmapDecodeLength(byte[] p, int start) {
		int len = p[start];
		if (len < ZIPMAP_BIGLEN) {
			return len;
		}
		len = ((p[start + 4] & 0x00ff) << 24) + ((p[start + 3] & 0x00ff) << 16)
				+ ((p[start + 2] & 0x00ff) << 8) + ((p[start + 1] & 0x00ff));
		return len;
	}

	static int zipmapEncodeLength(byte[] p, int len) {
		if (p == null) {
			return decodeFlagLen(len);
		} else {
			if (len < ZIPMAP_BIGLEN) {
				p[0] = (byte) len;
				return 1;
			} else {
				p[0] = (byte) ZIPMAP_BIGLEN;
				p[1] = (byte) (0x00ff & len);
				p[2] = (byte) ((0x00ff & len) >> 8);
				p[3] = (byte) ((0x00ff & len) >> 16);
				p[4] = (byte) ((0x00ff & len) >> 24);
				return 1 + 4;
			}
		}
	}

	/*
	 * byte����תΪ���ص�HashMap
	 */
	public static HashMap<String, String> zipmapExpand(byte[] zm) {
		byte[] p = zm;
		int l, llen;

		int pos = 1;
		HashMap<String, String> res = new HashMap<String, String>();
		while ((0x00ff & p[pos]) != ZIPMAP_END) {
			int free;
			l = zipmapDecodeLength(p, pos);
			llen = zipmapEncodeLength(null, l);
			pos += llen;
			String key = new String(zm, pos, l);
			pos += l;
			l = zipmapDecodeLength(p, pos);
			pos += zipmapEncodeLength(null, l);
			free = (0x00ff & p[pos]);
			pos += 1;
			String value = new String(zm, pos, l);
			pos += l;
			pos += free;
			res.put(key, value);
		}
		return res;
	}
}
