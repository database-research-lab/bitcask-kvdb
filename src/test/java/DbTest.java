import cc11001100.bitcask.kvdb.db.Db;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author CC11001100
 */
public class DbTest {

	private static Db db;

	@BeforeClass
	public static void beforeClass() {
		db = new Db("foo", "D:/test/bitcask/");
	}

	@Test
	public void testWriteAndRead() {
		db.put("key", "value");
		String value = db.get("key");
		System.out.println(value);
	}

}
