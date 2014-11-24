package com.pajk.rdb;

/**
 * 
 * ���������ֵĸߵ�λת��
 * 
 * @author Wang GangHua
 * @version 1.0.0 2013-11-30
 * 
 */
public class EndianFormat {
	/*
	 * ��16λ���ݽ��иߵ�λ�л�,��λתΪ��λ
	 */
	public static void memrev16(byte[] p, int start) {
		byte[] x = p;
		byte t;

		t = x[start];
		x[start] = x[start + 1];
		x[start + 1] = t;
	}

	/*
	 * ��32λ���ݽ��иߵ�λ�л�,��λתΪ��λ
	 */
	public static void memrev32(byte[] p, int start) {
		byte[] x = p;
		byte t;

		t = x[start];
		x[start] = x[start + 3];
		x[start + 3] = t;
		t = x[start + 1];
		x[start + 1] = x[start + 2];
		x[start + 2] = t;
	}

	/*
	 * ��64λ���ݽ��иߵ�λ�л�,��λתΪ��λ
	 */
	public static void memrev64(byte[] p, int start) {
		byte[] x = p;
		byte t;

		t = x[start];
		x[start] = x[start + 7];
		x[start + 7] = t;
		t = x[start + 1];
		x[start + 1] = x[start + 6];
		x[start + 6] = t;
		t = x[start + 2];
		x[start + 2] = x[start + 5];
		x[start + 5] = t;
		t = x[start + 3];
		x[start + 3] = x[start + 4];
		x[start + 4] = t;
	}
}