package com.pajk.rdb;

public interface CallBackHandler {

	public void printlnHandler(long dbid, String type, String key, Object val,
			long expiretime);
}
