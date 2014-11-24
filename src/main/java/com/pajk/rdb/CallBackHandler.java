package com.pajk.rdb;

public interface CallBackHandler {

	public void printlnHandler(long dbid, int type, String key, Object val,
			int vlen, long expiretime);
}
