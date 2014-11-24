package com.pajk.rdb;

public class CRC64 {
	/**
	 * CRC-CCITT(Kermit)验证模式
	 * 
	 * @param str
	 * @return
	 */
	public String CRC_CCITT_Kermit(String str) {
		int j, b, rrrc, c, i;
		String tmpBalance;
		int k;
		rrrc = 0;
		tmpBalance = str;
		int tmpInt, CharInt;
		String tmpChar, tmpStr;
		tmpStr = "";
		int High;
		int Low;
		for (j = 1; j <= 3; j++) {
			if (Character.isDigit(tmpBalance.charAt(2 * j - 2))) {
				High = Integer.parseInt(tmpBalance.charAt(2 * j - 2) + "");
			} else {
				High = 0;
			}
			if (Character.isDigit(tmpBalance.charAt(2 * j - 1))) {
				Low = Integer.parseInt(tmpBalance.charAt(2 * j - 1) + "");
			} else {
				Low = 0;
			}
			High = (High & 0xff) << 4;
			High = High | Low;
			k = High;
			for (i = 1; i <= 8; i++) {
				c = rrrc & 1;
				rrrc = rrrc >> 1;
				if ((k & 1) != 0) {
					rrrc = rrrc | 0x8000;
				}
				if (c != 0) {
					rrrc = rrrc ^ 0x8408;
				}
				k = k >> 1;
			}
		}
		for (i = 1; i <= 16; i++) {
			c = rrrc & 1;
			rrrc = rrrc >> 1;
			if (c != 0) {
				rrrc = rrrc ^ 0x8408;
			}
		}
		c = rrrc >> 8;
		b = rrrc << 8;
		rrrc = c | b;
		tmpInt = rrrc;
		tmpStr = "";
		for (i = 1; i <= 4; i++) {
			tmpChar = "";
			CharInt = tmpInt % 16;
			if (CharInt > 9) {
				switch (CharInt) {
				case 10:
					tmpChar = "A";
					break;
				case 11:
					tmpChar = "B";
					break;
				case 12:
					tmpChar = "C";
					break;
				case 13:
					tmpChar = "D";
					break;
				case 14:
					tmpChar = "E";
					break;
				case 15:
					tmpChar = "F";
					break;
				}
			} else {
				tmpChar = Integer.toString(CharInt);
			}
			tmpInt = tmpInt / 16;
			tmpStr = tmpChar + tmpStr;
		}
		System.out.println("tmpStr:" + tmpStr);
		return tmpStr;
	}

	/**
	 * CRC-CCITT(XModem) CRC-CCITT(0xFFFF) CRC-CCITT(0x1D0F) 校验模式
	 * 
	 * @param flag
	 *            < XModem(flag=1) 0xFFFF(flag=2) 0x1D0F(flag=3)>
	 * @param str
	 * @return
	 */
	public String CRC_CCITT(int flag, String str) {
		int crc = 0x00; // initial value
		int polynomial = 0x1021;
		byte[] bytes = str.getBytes();

		switch (flag) {
		case 1:
			crc = 0x00;
			break;
		case 2:
			crc = 0xFFFF;
			break;
		case 3:
			crc = 0x1D0F;
			break;

		}
		for (int index = 0; index < bytes.length; index++) {
			byte b = bytes[index];
			for (int i = 0; i < 8; i++) {
				boolean bit = ((b >> (7 - i) & 1) == 1);
				boolean c15 = ((crc >> 15 & 1) == 1);
				crc <<= 1;
				if (c15 ^ bit)
					crc ^= polynomial;
			}
		}
		crc &= 0xffff;
		str = Integer.toHexString(crc);
		return str;

	}
}