package cc11001100.bitcask.kvdb.db;

import cc11001100.bitcask.kvdb.entity.KeyIndex;
import cc11001100.bitcask.kvdb.utils.ByteUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import static cc11001100.bitcask.kvdb.utils.SerializeUtil.deserialize;
import static cc11001100.bitcask.kvdb.utils.SerializeUtil.serialize;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.io.FileUtils.readFileToString;
import static org.apache.commons.io.FileUtils.writeStringToFile;

/**
 * 用来方便操作数据库的数据文件的
 *
 * @author CC11001100
 */
public class DataFileManager {

	private static final Logger log = LogManager.getLogger(DataFileManager.class);

	/**
	 * 数据文件最大大小，当超过此大小时需要创建新的数据文件
	 */
	private static final Integer DEFAULT_DB_MAX_SIZE = 1024 * 1024 * 128; // 128M

	/**
	 * 数据库名称
	 */
	private String dbName;

	/**
	 * 数据文件的存放目录
	 */
	private String dataBasePath;

	/**
	 * 生成数据文件名时的自增id
	 */
	private Integer incrementId;

	/**
	 * 当前活跃数据库文件全路径
	 */
	private String activeDataCursor;

	/**
	 * 当前活跃文件的偏移量
	 */
	private long offset;

	/**
	 * @param dataBasePath 数据文件存放路径
	 */
	public DataFileManager(String dbName, String dataBasePath) {
		this.dbName = dbName;
		this.dataBasePath = dataBasePath;

		// 自动创建文件
		File dataBasePathFile = new File(dataBasePath);
		if (!dataBasePathFile.exists()) {
			dataBasePathFile.mkdirs();
		}

		// 初始化
		incrementId = readIncrementId();
		activeDataCursor = buildDataFilePath(incrementId);
		File activeDataFile = new File(activeDataCursor);
		if (activeDataFile.exists()) {
			offset = activeDataFile.length();
		} else {
			offset = 0;
		}
	}

	private String getIncrementIdFilePath() {
		return dataBasePath + "/increment-id";
	}

	private int readIncrementId() {
		try {
			File incrementIdFile = new File(getIncrementIdFilePath());
			if (incrementIdFile.exists()) {
				return Integer.parseInt(readFileToString(incrementIdFile, UTF_8));
			} else {
				// 第一次读取
				saveIncrementId(0);
				return 0;
			}
		} catch (IOException e) {
			log.error("read increment id file error");
		}
		return 0;
	}

	private void saveIncrementId(int incrementId) {
		try {
			writeStringToFile(new File(getIncrementIdFilePath()), Integer.toString(incrementId), UTF_8);
		} catch (IOException e) {
			log.error("write increment id file error");
		}
	}

	private String buildDataFilePath(int id) {
		return dataBasePath + "/" + dbName + "-data-" + id + ".db";
	}

	/**
	 * 将kv对落盘，返回其索引位置
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public <K, V> KeyIndex save(K key, V value) {
		// 只保存必须字段
		byte[] keyBytes = serialize(key);
		int keySize = keyBytes.length;
		byte[] valueBytes = serialize(value);
		int valueSize = valueBytes.length;
		int recordSaveSize = 4 + keySize + 4 + valueSize;
		long recordOffset = offset;

		// 落盘
		rotation();
		try {
			File activeDataCursorFile = new File(activeDataCursor);
			byte[] needWriteBytes = concat(keySize, keyBytes, valueSize, valueBytes);
			FileUtils.writeByteArrayToFile(activeDataCursorFile, needWriteBytes, true);
			offset += recordSaveSize;
		} catch (IOException e) {
			log.error("write db file {} error", activeDataCursor);
		}

		// 构建索引
		KeyIndex keyIndex = new KeyIndex();
		// 只保存文件名，这样每个key都可以将basepath节省掉
		keyIndex.setFileName(FilenameUtils.getName(activeDataCursor));
		keyIndex.setOffset(recordOffset);
		keyIndex.setSize(recordSaveSize);
		return keyIndex;
	}

	/**
	 * 将要写入的内容连接以便一次写入
	 *
	 * @param keySize
	 * @param keyBytes
	 * @param valueSize
	 * @param valueBytes
	 * @return
	 */
	private byte[] concat(int keySize, byte[] keyBytes, int valueSize, byte[] valueBytes) {
		int newByteArrayLength = 4 + keySize + 4 + valueSize;
		byte[] result = new byte[newByteArrayLength];
		int copyOffset = 0;

		byte[] keySizeBytes = ByteUtil.intToBytes(keySize);
		System.arraycopy(keySizeBytes, 0, result, copyOffset, keySizeBytes.length);
		copyOffset += keySizeBytes.length;

		System.arraycopy(keyBytes, 0, result, copyOffset, keyBytes.length);
		copyOffset += keyBytes.length;

		byte[] valueSizeBytes = ByteUtil.intToBytes(valueSize);
		System.arraycopy(valueSizeBytes, 0, result, copyOffset, valueSizeBytes.length);
		copyOffset += valueSizeBytes.length;

		System.arraycopy(valueBytes, 0, result, copyOffset, valueBytes.length);
		copyOffset += valueBytes.length;

		return result;
	}

	/**
	 * 检查当前活跃文件是否到达上限，若到达则进行活跃文件的轮转
	 */
	private void rotation() {
		// 借助offset来实现判断文件大小
		if (offset > DEFAULT_DB_MAX_SIZE) {
			activeDataCursor = nextDataFileName();
			offset = 0;
			saveIncrementId(incrementId);
		}
	}

	/**
	 * 当需要轮转时，生成下一个数据文件的名字
	 *
	 * @return
	 */
	private String nextDataFileName() {
		return buildDataFilePath(++incrementId);
	}

	/**
	 * 根据索引读取文件
	 *
	 * @param keyIndex
	 * @param <K>
	 * @param <V>
	 * @return
	 */
	public <K, V> V read(KeyIndex keyIndex) {
		byte[] saveContent = new byte[keyIndex.getSize()];
		try {
			String dbFilePath = dataBasePath + "/" + keyIndex.getFileName();
			FileInputStream fileInputStream = new FileInputStream(dbFilePath);
			IOUtils.skip(fileInputStream, keyIndex.getOffset());
			IOUtils.read(fileInputStream, saveContent);
		} catch (IOException e) {
			log.error("read data file {} error", keyIndex.getFileName());
			return null;
		}

		ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(saveContent);
		byte[] keySizeBytes = new byte[4];
		try {
			byteArrayInputStream.read(keySizeBytes);
			int keySize = ByteUtil.byteToInt(keySizeBytes);

			byte[] keyBytes = new byte[keySize];
			byteArrayInputStream.read(keyBytes);
//			K key = deserialize(keyBytes);

			byte[] valueSizeBytes = new byte[4];
			byteArrayInputStream.read(valueSizeBytes);
			int valueSize = ByteUtil.byteToInt(valueSizeBytes);

			byte[] valueBytes = new byte[valueSize];
			byteArrayInputStream.read(valueBytes);
			V value = deserialize(valueBytes);
			return value;
		} catch (IOException e) {
			log.error("deserialize error");
		}
		return null;
	}

}
