import cc11001100.bitcask.kvdb.db.DataFileManager;
import cc11001100.bitcask.kvdb.entity.KeyIndex;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;

/**
 * @author CC11001100
 */
public class DataFileManagerTest {

	private static DataFileManager dataFileManager;

	@BeforeClass
	public static void beforeClass() {
		dataFileManager = new DataFileManager("foo", "D:/test/bitcask");
	}

	/**
	 * 测试保存记录
	 */
	@Ignore
	@Test
	public void testSave() {
		KeyIndex keyIndex = dataFileManager.save("key", "value");
		System.out.println(keyIndex);
	}

	/**
	 * 测试读取记录
	 */
	@Ignore
	@Test
	public void testRead() {
		KeyIndex keyIndex = dataFileManager.save("key", "如果是保存中文呢");
		System.out.println("read readValue=" + dataFileManager.read(keyIndex));
	}

	@Ignore
	@Test
	public void testCrazySave() {
		dataFileManager.save("foo", "bar");
		Random random = new Random();
		long start = System.currentTimeMillis();
		for (int i = 0; i < 100; i++) {
			dataFileManager.save(random.nextLong(), random.nextLong());
		}
		long cost = System.currentTimeMillis() - start;
		System.out.println(cost);
	}

}
