import cc11001100.bitcask.kvdb.db.KeyIndexManager;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author CC11001100
 */
public class KeyIndexManagerTest {

	private static KeyIndexManager keyIndexManager;

	@BeforeClass
	public static void beforeClass() {
		keyIndexManager = new KeyIndexManager("foo", "D:/test/bitcask/");
	}

	@Test
	public void testCreateKeyIndexFromDbFile() {
		keyIndexManager.createKeyIndexForDbFile("foo-data-0.db").forEach((k, v) -> {
			System.out.println(k + "=" + v);
		});
	}

}
