package com.googlecode.connectlet;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.Random;

public class Bytes {
	public static final boolean BIG_ENDIAN = false;
	public static final boolean LITTLE_ENDIAN = true;

	public static final byte[] EMPTY_BYTES = new byte[0];

	private static final byte[] HEX_UPPER_CHAR = "0123456789ABCDEF".getBytes();
	private static final byte[] HEX_LOWER_CHAR = "0123456789abcdef".getBytes();

	private static final byte[] HEX_DECODE_CHAR = {
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  1,  2,  3,  4,  5,  6,  7,  8,  9,  0,  0,  0,  0,  0,  0,
		0, 10, 11, 12, 13, 14, 15,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0, 10, 11, 12, 13, 14, 15,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
		0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	};

	public static byte[] fromHex(String hex) {
		byte[] bHex = hex.getBytes();
		byte[] b = new byte[bHex.length / 2];
		for (int i = 0; i < b.length; i ++) {
			b[i] = (byte) (HEX_DECODE_CHAR[bHex[i * 2]] * 16 + HEX_DECODE_CHAR[bHex[i * 2 + 1]]);
		}
		return b;
	}

	public static String toHexUpper(byte[] b) {
		return toHexUpper(b, 0, b.length);
	}

	public static String toHexUpper(byte[] b, int off, int len) {
		byte[] hex = new byte[len * 2];
		for (int i = 0; i < len; i ++) {
			hex[i * 2] = HEX_UPPER_CHAR[(b[off + i] & 0xf0) >>> 4];
			hex[i * 2 + 1] = HEX_UPPER_CHAR[b[off + i] & 0xf];
		}
		return new String(hex);
	}

	public static String toHexLower(byte[] b) {
		return toHexLower(b, 0, b.length);
	}

	public static String toHexLower(byte[] b, int off, int len) {
		byte[] hex = new byte[len * 2];
		for (int i = 0; i < len; i ++) {
			hex[i * 2] = HEX_LOWER_CHAR[(b[off + i] & 0xf0) >>> 4];
			hex[i * 2 + 1] = HEX_LOWER_CHAR[b[off + i] & 0xf];
		}
		return new String(hex);
	}

	public static byte[] fromShort(int n) {
		return fromShort(n, BIG_ENDIAN);
	}

	public static byte[] fromShort(int n, boolean bLittleEndian) {
		byte[] b = new byte[2];
		setShort(n, b, 0, bLittleEndian);
		return b;
	}

	public static void setShort(int n, byte[] b, int off) {
		setShort(n, b, off, BIG_ENDIAN);
	}

	public static void setShort(int n, byte[] b, int off, boolean bLittleEndian) {
		if (bLittleEndian) {
			b[off] = (byte) n;
			b[off + 1] = (byte) (n >>> 8);
		} else {
			b[off] = (byte) (n >>> 8);
			b[off + 1] = (byte) n;
		}
	}

	public static byte[] fromInt(int n) {
		return fromInt(n, BIG_ENDIAN);
	}

	public static byte[] fromInt(int n, boolean bLittleEndian) {
		byte[] b = new byte[4];
		setInt(n, b, 0, bLittleEndian);
		return b;
	}

	public static void setInt(int n, byte[] b, int off) {
		setInt(n, b, off, BIG_ENDIAN);
	}

	public static void setInt(int n, byte[] b, int off, boolean bLittleEndian) {
		if (bLittleEndian) {
			b[off] = (byte) n;
			b[off + 1] = (byte) (n >>> 8);
			b[off + 2] = (byte) (n >>> 16);
			b[off + 3] = (byte) (n >>> 24);
		} else {
			b[off] = (byte) (n >>> 24);
			b[off + 1] = (byte) (n >>> 16);
			b[off + 2] = (byte) (n >>> 8);
			b[off + 3] = (byte) n;
		}
	}

	public static byte[] fromLong(long n) {
		return fromLong(n, BIG_ENDIAN);
	}

	public static byte[] fromLong(long n, boolean bLittleEndian) {
		byte[] b = new byte[8];
		setLong(n, b, 0, bLittleEndian);
		return b;
	}

	public static void setLong(long n, byte[] b, int off) {
		setLong(n, b, off, BIG_ENDIAN);
	}

	public static void setLong(long n, byte[] b, int off, boolean bLittleEndian) {
		if (bLittleEndian) {
			setInt((int) n, b, off, LITTLE_ENDIAN);
			setInt((int) (n >>> 32), b, off + 4, LITTLE_ENDIAN);
		} else {
			setInt((int) (n >>> 32), b, off, BIG_ENDIAN);
			setInt((int) n, b, off + 4, BIG_ENDIAN);
		}
	}

	public static int toShort(byte[] b) {
		return toShort(b, 0);
	}

	public static int toShort(byte[] b, boolean bLittleEndian) {
		return toShort(b, 0, bLittleEndian);
	}

	public static int toShort(byte[] b, int off) {
		return toShort(b, off, BIG_ENDIAN);
	}

	public static int toShort(byte[] b, int off, boolean bLittleEndian) {
		if (bLittleEndian) {
			return ((b[off] & 0xff) | ((b[off + 1] & 0xff) << 8));
		}
		return (((b[off] & 0xff) << 8) | (b[off + 1] & 0xff));
	}

	public static int toInt(byte[] b) {
		return toInt(b, 0);
	}

	public static int toInt(byte[] b, boolean bLittleEndian) {
		return toInt(b, 0, bLittleEndian);
	}

	public static int toInt(byte[] b, int off) {
		return toInt(b, off, BIG_ENDIAN);
	}

	public static int toInt(byte[] b, int off, boolean bLittleEndian) {
		if (bLittleEndian) {
			return (b[off] & 0xff) | ((b[off + 1] & 0xff) << 8) | ((b[off + 2] & 0xff) << 16) | ((b[off + 3] & 0xff) << 24);
		}
		return ((b[off] & 0xff) << 24) | ((b[off + 1] & 0xff) << 16) | ((b[off + 2] & 0xff) << 8) | (b[off + 3] & 0xff);
	}

	public static long toLong(byte[] b) {
		return toLong(b, 0);
	}

	public static long toLong(byte[] b, boolean bLittleEndian) {
		return toLong(b, 0, bLittleEndian);
	}

	public static long toLong(byte[] b, int off) {
		return toLong(b, off, BIG_ENDIAN);
	}

	public static long toLong(byte[] b, int off, boolean bLittleEndian) {
		if (bLittleEndian) {
			return (toInt(b, off, LITTLE_ENDIAN) & 0xffffffffL) | ((toInt(b, off + 4, LITTLE_ENDIAN) & 0xffffffffL) << 32);
		}
		return ((toInt(b, off, BIG_ENDIAN) & 0xffffffffL) << 32) | (toInt(b, off + 4, BIG_ENDIAN) & 0xffffffffL);
	}

	public static byte[] fromBigInteger(BigInteger bi, int len) {
		byte[] b = bi.toByteArray();
		byte[] result = new byte[len];
		if (b.length < len) {
			for (int i = 0; i < len - b.length; i ++) {
				result[i] = 0;
			}
			System.arraycopy(b, 0, result, len - b.length, b.length);
		} else {
			System.arraycopy(b, b.length - len, result, 0, len);
		}
		return result;
	}

	public static byte[] add(byte[] b1, int off1, int len1, byte[] b2, int off2, int len2) {
		byte[] b = new byte[len1 + len2];
		System.arraycopy(b1, off1, b, 0, len1);
		System.arraycopy(b2, off2, b, len1, len2);
		return b;
	}

	public static byte[] add(byte[]... b) {
		int nLen = 0;
		for (int i = 0; i < b.length; i ++) {
			nLen += b[i].length;
		}
		byte[] lp = new byte[nLen];
		nLen = 0;
		for (int i = 0; i < b.length; i ++) {
			byte[] lpi = b[i];
			System.arraycopy(lpi, 0, lp, nLen, lpi.length);
			nLen += lpi.length;
		}
		return lp;
	}

	public static byte[] clone(byte[] b) {
		return sub(b, 0, b.length);
	}

	public static byte[] left(byte[] b, int len) {
		return sub(b, 0, len);
	}

	public static byte[] right(byte[] b, int len) {
		return sub(b, b.length - len, len);
	}

	public static byte[] sub(byte[] b, int off) {
		return sub(b, off, b.length - off);
	}

	public static byte[] sub(byte[] b, int off, int len) {
		byte[] result = new byte[len];
		System.arraycopy(b, off, result, 0, len);
		return result;
	}

	public static boolean equals(byte[] b1, int off1, byte[] b2, int off2, int len) {
		if (b1 == b2 && off1 == off2) {
			return true;
		}
		for (int i = 0; i < len; i ++) {
			if (b1[off1 + i] != b2[off2 + i]) {
				return false;
			}
		}
		return true;
	}

	public static boolean equals(byte[] b1, byte[] b2) {
		return b1.length == b2.length && equals(b1, 0, b2, 0, b1.length);
	}

	private static Random random = new Random();
	private static SecureRandom secureRandom = new SecureRandom();

	private static byte[] random(int len, Random random_) {
		byte[] result = new byte[len];
		random_.nextBytes(result);
		return result;
	}

	public static byte[] random(int len) {
		return random(len, random);
	}

	public static byte[] secureRandom(int len) {
		return random(len, secureRandom);
	}

	private static final byte[] HEX_CHAR = "0123456789ABCDEF".getBytes();

	private static String dumpLine(byte[] b, int offDiv16, int begin, int end) {
		byte[] lpLine = "                                                                             ".getBytes();
		//				"0000:0000  00 00 00 00 00 00 00 00-00 00 00 00 00 00 00 00   ................"
		lpLine[0] = HEX_CHAR[(offDiv16 >>> 24) & 0xf];
		lpLine[1] = HEX_CHAR[(offDiv16 >>> 20) & 0xf];
		lpLine[2] = HEX_CHAR[(offDiv16 >>> 16) & 0xf];
		lpLine[3] = HEX_CHAR[(offDiv16 >>> 12) & 0xf];
		lpLine[4] = ':';
		lpLine[5] = HEX_CHAR[(offDiv16 >>> 8) & 0xf];
		lpLine[6] = HEX_CHAR[(offDiv16 >>> 4) & 0xf];
		lpLine[7] = HEX_CHAR[offDiv16 & 0xf];
		lpLine[8] = '0';
		for (int i = 0; i < 16; i ++) {
			if (i >= begin && i < end) {
				int bInt = b[offDiv16 * 16 + i];
				lpLine[i * 3 + 11] = HEX_CHAR[(bInt & 0xf0) >> 4];
				lpLine[i * 3 + 12] = HEX_CHAR[bInt & 0xf];
				if (bInt >= 32 && bInt < 127) {
					lpLine[61 + i] = (byte) bInt;
				} else {
					lpLine[61 + i] = '.';
				}
			}
		}
		if (begin < 8 && end > 8) {
			lpLine[34] = '-';
		}
		return new String(lpLine);
	}

	public static void dump(PrintStream out, byte[] b, int off, int len) {
		int end = off + len;
		int offDiv16 = off / 16;
		int endDiv16 = end / 16;
		if (offDiv16 == endDiv16) {
			out.println(dumpLine(b, offDiv16, off % 16, end % 16));
		} else {
			out.println(dumpLine(b, offDiv16, off % 16, 16));
			for (int i = offDiv16 + 1; i < endDiv16; i ++) {
				out.println(dumpLine(b, i, 0, 16));
			}
			if (end % 16 > 0) {
				out.println(dumpLine(b, endDiv16, 0, end % 16));
			}
		}
	}

	public static void dump(PrintStream out, byte[] b) {
		dump(out, b, 0, b.length);
	}

	public static void dump(PrintWriter out, byte[] b, int off, int len) {
		int end = off + len;
		int offDiv16 = off / 16;
		int endDiv16 = end / 16;
		if (offDiv16 == endDiv16) {
			out.println(dumpLine(b, offDiv16, off % 16, end % 16));
		} else {
			out.println(dumpLine(b, offDiv16, off % 16, 16));
			for (int i = offDiv16 + 1; i < endDiv16; i ++) {
				out.println(dumpLine(b, i, 0, 16));
			}
			if (end % 16 > 0) {
				out.println(dumpLine(b, endDiv16, 0, end % 16));
			}
		}
	}

	public static void dump(PrintWriter out, byte[] b) {
		dump(out, b, 0, b.length);
	}

	public static void xor(byte[] src, int srcPos, byte[] dest, int destPos, int length) {
		for (int i = 0; i < length; i ++) {
			dest[destPos + i] ^= src[srcPos + i];
		}
	}

	public static void xor(byte[] src1, int src1Pos, byte[] src2, int src2Pos,
			byte[] dest, int destPos, int length) {
		for (int i = 0; i < length; i ++) {
			dest[destPos + i] = (byte) (src1[src1Pos + i] ^ src2[src2Pos + i]);
		}
	}
}