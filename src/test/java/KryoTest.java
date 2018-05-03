import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Ignore;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * @author CC11001100
 */
public class KryoTest {

	@Ignore
	@Test
	public void testWriteObjectAndClass() throws FileNotFoundException {
		String savaPath = "D:/test/bitcask/kryo_test.bin";

		Map<String, String> map = new HashMap<>();
		map.put("key", "value");

		Kryo kryo = new Kryo();
		Output output = new Output(new FileOutputStream(savaPath));
		kryo.writeClassAndObject(output, map);
		output.close();

		Input input = new Input(new FileInputStream(savaPath));
		System.out.println(kryo.readClassAndObject(input));
		input.close();

	}

}
