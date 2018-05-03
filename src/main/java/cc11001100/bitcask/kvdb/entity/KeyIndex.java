package cc11001100.bitcask.kvdb.entity;

/**
 * 索引，用来将key指向value在磁盘上的位置
 *
 * @author CC11001100
 */
public class KeyIndex {

	/**
	 * value所在的文件名称
	 */
	private String fileName;

	/**
	 * 偏移量
	 */
	private Long offset;

	/**
	 * 有多长
	 */
	private Integer size;

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public Long getOffset() {
		return offset;
	}

	public void setOffset(Long offset) {
		this.offset = offset;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	@Override
	public String toString() {
		return "KeyIndex{" +
				"fileName='" + fileName + '\'' +
				", offset=" + offset +
				", size=" + size +
				'}';
	}
}
