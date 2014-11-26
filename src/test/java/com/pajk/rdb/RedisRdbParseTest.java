package com.pajk.rdb;

import java.io.File;

public class RedisRdbParseTest {
	public static void main(String[] args) {
		String path = RedisRdbParseTest.class.getResource("").getPath()
				+ "dump3.0.rdb";
		RdbParser rdb = new RdbParser();
		rdb.rdbParse(new File(path), new CallBackHandler() {
			public void printlnHandler(long dbid, String type, String key,
					Object val, long expiretime) {
				System.out.println(dbid + "||" + type + "||" + "key=" + key
						+ "||" + "value=" + val + "||" + expiretime);
			}
		});
	}
}
