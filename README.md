## 基于BitCask模型实现的kv数据库


### 版本迭代
Version 0.1 
- 单线程，无事务支持
- 定时gc（暂未实现）
- key,value类型仅支持字符串类型
- 数据文件使用序列化方式存储，无压缩


### 数据文件的格式
```
crc 后面所有字段的crc校验
// crc 不加
// timestamp 落盘时间 不加
4 byte key_size
n byte key
4 byte value_size
n byte value
....
```













