package cc11001100.bitcask.kvdb.db;

import cc11001100.bitcask.kvdb.entity.KeyIndex;
import cc11001100.bitcask.kvdb.utils.ByteUtil;
import cc11001100.bitcask.kvdb.utils.SerializeUtil;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.readFileToString;

/**
 * 用来操作数据库的key索引
 *
 * @author CC11001100
 */
public class KeyIndexManager {

	private static final Logger log = LogManager.getLogger(KeyIndexManager.class);

	/**
	 * 数据库的名称
	 */
	private String dbName;

	/**
	 * 数据文件的存放目录
	 */
	private String dbDataBasePath;

	public KeyIndexManager(String dbName, String dbDataBasePath) {
		this.dbName = dbName;
		this.dbDataBasePath = dbDataBasePath;
	}

	/**
	 * 从hit file 恢复索引
	 *
	 * @return
	 */
	public <K> Map<K, KeyIndex> recoveryFromHitFileIfExist() {
		// TODO 待优化，先简单实现
		try {
			byte[] keyIndexBytes = FileUtils.readFileToByteArray(new File(getHitFilePath()));
			return SerializeUtil.deserialize(keyIndexBytes);
		} catch (IOException e) {
			log.error("write hit file error");
		}
		return null;
	}

	/**
	 * 保存hit file，以便下次使用
	 *
	 * @param indexMap
	 * @return
	 */
	public <K> void saveHitFile(Map<K, KeyIndex> indexMap) {
		// TODO 待优化，先简单实现
		byte[] keyIndexBytes = SerializeUtil.serialize(indexMap);
		try {
			FileUtils.writeByteArrayToFile(new File(getHitFilePath()), keyIndexBytes);
		} catch (IOException e) {
			log.error("write hit file error");
		}
	}

	private String getHitFilePath() {
		return dbDataBasePath + "/" + dbName + "-hit.index";
	}

	/**
	 * 为数据文件创建索引
	 *
	 * @param dataFileName
	 * @param <K>
	 * @return
	 */
	public <K> Map<K, KeyIndex> createKeyIndexForDbFile(String dataFileName) {
		String fqdn = dbDataBasePath + "/" + dataFileName;
		Map<K, KeyIndex> keyIndexMap = new HashMap<>();
		try {
			FileInputStream fis = new FileInputStream(fqdn);
			long offset = 0;
			byte[] buffer = new byte[1024 * 100];
			while (fis.read(buffer, 0, 4) != -1) {
				int keySize = ByteUtil.byteToInt(buffer, 4);
				fis.read(buffer, 0, keySize);
				K key = SerializeUtil.deserialize(buffer);

				fis.read(buffer, 0, 4);
				int valueSize = ByteUtil.byteToInt(buffer, 4);
				fis.read(buffer, 0, valueSize);

				KeyIndex keyIndex = new KeyIndex();
				keyIndex.setFileName(dataFileName);
				int recordSize = 4 + keySize + 4 + valueSize;
				keyIndex.setSize(recordSize);
				keyIndex.setOffset(offset);
				keyIndexMap.put(key, keyIndex);

				offset += recordSize;
			}
		} catch (FileNotFoundException e) {
			log.error(" db file {} not found", dataFileName);
		} catch (IOException e) {
			log.error("read db file {} error", dataFileName);
		}
		return keyIndexMap;
	}

	public <K> Map<K, KeyIndex> initKeyIndexMap() {
		File hitFile = new File(getHitFilePath());
		if (hitFile.exists()) {
			return recoveryFromHitFileIfExist();
		} else {
			return createKeyIndexForDb();
		}
	}

	private <K> Map<K, KeyIndex> createKeyIndexForDb() {
		Map<K, KeyIndex> resultMap = new HashMap<>();
		File incrementIdFile = new File(getIncrementIdFilePath());
		if (!incrementIdFile.exists()) {
			return resultMap;
		}
		int incrementId = 0;
		try {
			incrementId = Integer.parseInt(FileUtils.readFileToString(incrementIdFile, UTF_8));
		} catch (IOException e) {
			e.printStackTrace();
		}

		for (int i = 0; i <= incrementId; i++) {
			String dataFileName = buildDataFilePath(i);
			resultMap.putAll(createKeyIndexForDbFile(dataFileName));
		}
		return resultMap;
	}

	private String getIncrementIdFilePath() {
		return dbDataBasePath + "/increment-id";
	}

	private String buildDataFilePath(int id) {
		return dbName + "-data-" + id + ".db";
	}

}
