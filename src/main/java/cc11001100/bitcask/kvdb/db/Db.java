package cc11001100.bitcask.kvdb.db;

import cc11001100.bitcask.kvdb.gc.Gc;
import cc11001100.bitcask.kvdb.entity.KeyIndex;

import java.util.HashMap;
import java.util.Map;

/**
 * @author CC11001100
 */
public class Db {

	/**
	 * 数据库名字
	 */
	private String name;

	/**
	 * 数据及索引等相关文件的存储路径
	 */
	private String dbPath;

	/**
	 * 存储索引的map
	 */
	private Map<Object, KeyIndex> keyIndexMap = new HashMap<>();

	private DataFileManager dataFileManager;
	private KeyIndexManager keyIndexManager;
	private Gc gc = new Gc(this);

	public Db(String dbName, String dbPath) {
		this.dbPath = dbPath;
		this.name = dbName;

		dataFileManager = new DataFileManager(name, dbPath);
		keyIndexManager = new KeyIndexManager(name, dbPath);

		keyIndexMap = keyIndexManager.initKeyIndexMap();
	}

	/**
	 * 写
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public <K, V> boolean put(K key, V value) {
		try {
			KeyIndex keyIndex = dataFileManager.save(key, value);
			keyIndexMap.put(key, keyIndex);
			return true;
		} catch (Exception ignored) {
		}
		return false;
	}

	/**
	 * 读
	 *
	 * @param key
	 * @param <V>
	 * @return
	 */
	public <K, V> V get(K key) {
		KeyIndex keyIndex = keyIndexMap.get(key);
		if (keyIndex == null) {
			return null;
		} else {
			return dataFileManager.read(keyIndex);
		}
	}

	/**
	 * gc优化存储
	 */
	public void gc() {

	}

}
