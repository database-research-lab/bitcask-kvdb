package cc11001100.bitcask.kvdb.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * @author CC11001100
 */
public class SerializeUtil {

	private static Kryo kryo = new Kryo();

	/**
	 * 将对象序列化为字节数组
	 *
	 * @param o
	 * @return
	 */
	public static byte[] serialize(Object o) {
		ByteArrayOutputStream result = new ByteArrayOutputStream();
		Output output = new Output(result);
		kryo.writeClassAndObject(output, o);
		output.close();
		return result.toByteArray();
	}

	/**
	 * 从字节数组反序列化出对象
	 *
	 * @param serializeBytes
	 * @param <T>
	 * @return
	 */
	public static <T> T deserialize(byte[] serializeBytes) {
		ByteArrayInputStream inputStream = new ByteArrayInputStream(serializeBytes);
		Input input = new Input(inputStream);
		T result = (T) kryo.readClassAndObject(input);
		input.close();
		return result;
	}

}
