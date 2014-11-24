package com.pajk.rdb;

import java.io.File;

public class Test {
	public static void main(String[] args) {
		String filePath = "D:\\git\\rdbtools\\tests\\dump2.8.rdb";
		RdbParser rdb = new RdbParser();
		rdb.rdbParse(new File(filePath), new CallBackHandler() {
			public void printlnHandler(long dbid, int type, String key,
					Object val, int vlen, long expiretime) {
				System.out.println(dbid + "||" + key + "||" + val + "||" + vlen
						+ "||" + expiretime);
			}
		});
	}
}
