package cc11001100.bitcask.kvdb.utils;

import java.nio.ByteBuffer;

/**
 * @author CC11001100
 */
public class ByteUtil {

	/**
	 * int to byte array
	 *
	 * @param n
	 * @return
	 */
	public static byte[] intToBytes(int n) {
		return ByteBuffer.allocate(4).putInt(n).array();
	}

	/**
	 * byte array to int
	 *
	 * @param bytes
	 * @return
	 */
	public static int byteToInt(byte[] bytes, int n) {
		int result = 0;
		for (int i = n - 1; i >= 0; i--) {
			int weight = 8 * (bytes.length - i - 1);
			result = result + (bytes[i] << weight);
		}
		return result;
	}

	public static int byteToInt(byte[] bytes) {
		return byteToInt(bytes, bytes.length);
	}

}
