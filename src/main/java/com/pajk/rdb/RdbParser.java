package com.pajk.rdb;

import static com.pajk.rdb.ByteUtils.chars2bytes;
import static com.pajk.rdb.ByteUtils.memcmp;
import static com.pajk.rdb.ByteUtils.readBytes;
import static com.pajk.rdb.ByteUtils.strtol;
import static com.pajk.rdb.RdbCommonUtils.REDIS_EOF;
import static com.pajk.rdb.RdbCommonUtils.REDIS_EXPIRETIME_FC;
import static com.pajk.rdb.RdbCommonUtils.REDIS_EXPIRETIME_FD;
import static com.pajk.rdb.RdbCommonUtils.REDIS_HASH;
import static com.pajk.rdb.RdbCommonUtils.REDIS_HASH_ZIPLIST;
import static com.pajk.rdb.RdbCommonUtils.REDIS_HASH_ZIPMAP;
import static com.pajk.rdb.RdbCommonUtils.REDIS_LIST;
import static com.pajk.rdb.RdbCommonUtils.REDIS_LIST_ZIPLIST;
import static com.pajk.rdb.RdbCommonUtils.REDIS_SELECTDB;
import static com.pajk.rdb.RdbCommonUtils.REDIS_SET;
import static com.pajk.rdb.RdbCommonUtils.REDIS_SET_INTSET;
import static com.pajk.rdb.RdbCommonUtils.REDIS_STRING;
import static com.pajk.rdb.RdbCommonUtils.REDIS_ZSET;
import static com.pajk.rdb.RdbCommonUtils.REDIS_ZSET_ZIPLIST;
import static com.pajk.rdb.RdbCommonUtils.rdbLoadDoubleValue;
import static com.pajk.rdb.RdbCommonUtils.rdbLoadStringObject;
import static com.pajk.rdb.RdbCommonUtils.rdbLoadTime;
import static com.pajk.rdb.RdbCommonUtils.rdbLoadType;
import static com.pajk.rdb.RdbCommonUtils.read_length_with_encoding;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

public class RdbParser {

	private static void ERROR(String msg, Object... args) {
		throw new RuntimeException(String.format(msg, args));
	}

	public void rdbParse(File file, CallBackHandler handler) {
		RandomAccessFile position = null;
		try {
			position = new RandomAccessFile(file, "r");
			rdbParseHeader(position);
			rdbParseBody(position, handler);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (RuntimeException e) {
			e.printStackTrace();
		} finally {
			if (position != null) {
				try {
					position.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		}
	}

	private void rdbParseBody(RandomAccessFile position, CallBackHandler handler) {
		int type;
		long expiretime = 0, dbid = -1;
		String key, valType;
		Object value;
		while (true) {
			type = rdbLoadType(position);
			if (type == REDIS_EXPIRETIME_FD) {
				if ((expiretime = rdbLoadTime(position, type)) == -1)
					ERROR("Could not read type");
				if ((type = rdbLoadType(position)) == -1)
					ERROR("Could not read type");
				expiretime *= 1000;
			} else if (type == REDIS_EXPIRETIME_FC) {
				if ((expiretime = rdbLoadTime(position, type)) == -1)
					ERROR("Could not read type");
				if ((type = rdbLoadType(position)) == -1)
					ERROR("Could not read type");
			}
			if (type == REDIS_EOF) {
				break;
			}
			if (type == REDIS_SELECTDB) {
				dbid = (Long) read_length_with_encoding(position)[0];
				continue;
			}
			key = (String) rdbLoadStringObject(position, true);
			if (key == null)
				ERROR("Could not read key");
			if (type == REDIS_HASH_ZIPMAP || type == REDIS_HASH_ZIPLIST) {
				valType = "HASH";
			} else if (type == REDIS_LIST_ZIPLIST) {
				valType = "LIST";
			} else if (type == REDIS_SET_INTSET) {
				valType = "SET";
			} else if (type == REDIS_ZSET_ZIPLIST) {
				valType = "ZSET";
			} else {
				valType = "STRING";
			}
			value = rdbLoadValueObject(position, type);
			handler.printlnHandler(dbid, valType, key, value, expiretime);
		}
	}

	private void rdbParseHeader(RandomAccessFile position) {
		byte[] buf = new byte[9];
		int dump_version;
		if (!readBytes(position, buf, 0, 9)) {
			ERROR("Cannot read header\n");
		}
		if (memcmp(buf, 0, chars2bytes("REDIS"), 0, 5) != 0) {
			ERROR("Wrong signature in header\n");
		}
		dump_version = (int) strtol(buf, 5, 10);
		if (dump_version < 1 || dump_version > 6) {
			ERROR("Unknown RDB format version: %d\n", dump_version);
		}
	}

	public Object rdbLoadValueObject(RandomAccessFile file, int type) {
		long length = 0;
		if (type == REDIS_LIST || type == REDIS_SET || type == REDIS_ZSET
				|| type == REDIS_HASH) {
			length = (Long) read_length_with_encoding(file)[0];
		}
		Object value = null;
		switch (type) {
		case REDIS_STRING:
			value = (String) rdbLoadStringObject(file, true);
			break;
		case REDIS_LIST:
			List<String> listValues = new ArrayList<String>();
			for (int i = 0; i < length; i++) {
				String val = (String) rdbLoadStringObject(file, true);
				listValues.add(val);
			}
			value = listValues;
			break;
		case REDIS_SET:
			Set<String> setValues = new HashSet<String>();
			for (int i = 0; i < length; i++) {
				String val = (String) rdbLoadStringObject(file, true);
				setValues.add(val);
			}
			value = setValues;
			break;
		case REDIS_ZSET:
			TreeMap<Double, String> zsetValues = new TreeMap<Double, String>();
			for (int i = 0; i < length; i++) {
				String val = (String) rdbLoadStringObject(file, true);
				Double score = rdbLoadDoubleValue(file);
				zsetValues.put(score, val);
			}
			value = zsetValues;
			break;
		case REDIS_HASH:
			HashMap<String, String> mapValues = new HashMap<String, String>();
			for (int i = 0; i < length; i++) {
				String mapKey = (String) rdbLoadStringObject(file, true);
				String mapValue = (String) rdbLoadStringObject(file, true);
				mapValues.put(mapKey, mapValue);
			}
			value = mapValues;
			break;
		default:
			byte[] strByte = (byte[]) rdbLoadStringObject(file, false);
			if (type == REDIS_HASH_ZIPMAP) {
				value = ZipMap.zipmapExpand(strByte);
			} else if (type == REDIS_LIST_ZIPLIST) {
				List<Object> lists = new ArrayList<Object>();
				ZipList zipList = new ZipList(strByte);
				int entryCountList = zipList.decodeEntryCount();
				for (int j = 0; j < entryCountList; j++) {
					lists.add(zipList.decodeEntryValue());
				}
				zipList.getEndByte();
				value = lists;
			} else if (type == REDIS_SET_INTSET) {
				IntSet intset = new IntSet(strByte);
				value = intset.docodeIntsetValue();
			} else if (type == REDIS_ZSET_ZIPLIST) {
				HashMap<Object, Object> zsetValue = new HashMap<Object, Object>();
				ZipList zipList = new ZipList(strByte);
				int entryCountList = zipList.decodeEntryCount();
				for (int j = 0; j < entryCountList / 2; j++) {
					Object val = zipList.decodeEntryValue();
					Object score = zipList.decodeEntryValue();
					if (score instanceof java.lang.Double) {
						score = (Double) score;
					} else if (score instanceof java.lang.Integer) {
						score = Float.valueOf(((Integer) score).toString());
					}
					zsetValue.put(val, score);
				}
				zipList.getEndByte();
				value = zsetValue;
			} else if (type == REDIS_HASH_ZIPLIST) {
				HashMap<Object, Object> hashValues = new HashMap<Object, Object>();
				ZipList zipList = new ZipList(strByte);
				int entryCountList = zipList.decodeEntryCount();
				for (int j = 0; j < entryCountList / 2; j++) {
					Object hashKey = zipList.decodeEntryValue();
					Object hashValue = zipList.decodeEntryValue();
					hashValues.put(hashKey, hashValue);
				}
				zipList.getEndByte();
				value = hashValues;
			}
			break;
		}

		return value;

	}
}
