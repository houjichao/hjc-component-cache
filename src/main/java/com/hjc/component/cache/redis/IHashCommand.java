package com.hjc.component.cache.redis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Redis 哈希(Hash)命令
 * 
 * @author hjc
 *
 */
public interface IHashCommand {
	/**
	 * Redis Hdel 命令用于删除哈希表 key 中的一个或多个指定字段，不存在的字段将被忽略。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            一个或多个指定字段
	 * @return 被成功删除字段的数量，不包括被忽略的字段。
	 */
	Long hdel(byte[] key, byte[]... field);

	/**
	 * Redis Hexists 命令用于查看哈希表的指定字段是否存在。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            字段
	 * @return 如果哈希表含有给定字段，返回 true 。 如果哈希表不含有给定字段，或 key 不存在，返回 false。
	 */
	Boolean hexists(byte[] key, byte[] field);

	/**
	 * Redis Hget 命令用于返回哈希表中指定字段的值。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            字段
	 * @return 返回给定字段的值。如果给定的字段或 key 不存在时，返回 nil 。
	 */
	byte[] hget(byte[] key, byte[] field);

	/**
	 * Redis Hgetall 命令用于返回哈希表中，所有的字段和值。<br/>
	 * 在返回值里，紧跟每个字段名(field name)之后是字段的值(value)，所以返回值的长度是哈希表大小的两倍。
	 * 
	 * @param key
	 *            KEY
	 * @return 以列表形式返回哈希表的字段及字段值。 若 key 不存在，返回空列表。
	 */
	Map<byte[], byte[]> hgetAll(byte[] key);

	/**
	 * Redis Hincrby 命令用于为哈希表中的字段值加上指定增量值。<br/>
	 * 增量也可以为负数，相当于对指定字段进行减法操作。<br/>
	 * 如果哈希表的 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。<br/>
	 * 如果指定的字段不存在，那么在执行命令前，字段的值被初始化为 0 。<br/>
	 * 对一个储存字符串值的字段执行 HINCRBY 命令将造成一个错误。<br/>
	 * 本操作的值被限制在 64 位(bit)有符号数字表示之内。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            字段
	 * @param value
	 *            增量值
	 * @return 执行 HINCRBY 命令之后，哈希表中字段的值。
	 */
	Long hincrBy(byte[] key, byte[] field, long value);

	/**
	 * Redis Hincrbyfloat 命令用于为哈希表中的字段值加上指定浮点数增量值。<br/>
	 * 如果指定的字段不存在，那么在执行命令前，字段的值被初始化为 0 。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            字段
	 * @param value
	 *            浮点数增量值
	 * @return 执行 Hincrbyfloat 命令之后，哈希表中字段的值。
	 */
	Double hincrByFloat(byte[] key, byte[] field, double value);

	/**
	 * Redis Hkeys 命令用于获取哈希表中的所有字段名。
	 * 
	 * @param key
	 *            KEY
	 * @return 包含哈希表中所有字段的列表。 当 key 不存在时，返回一个空列表。
	 */
	Set<byte[]> hkeys(byte[] key);

	/**
	 * Redis Hlen 命令用于获取哈希表中字段的数量。
	 * 
	 * @param key
	 *            KEY
	 * @return 哈希表中字段的数量。 当 key 不存在时，返回 0 。
	 */
	Long hlen(byte[] key);

	/**
	 * Redis Hmget 命令用于返回哈希表中，一个或多个给定字段的值。<br/>
	 * 如果指定的字段不存在于哈希表，那么返回一个 nil 值。
	 * 
	 * @param key
	 *            KEY
	 * @param fields
	 *            一个或多个给定字段
	 * @return 一个或多个给定字段的值
	 */
	List<byte[]> hmget(byte[] key, byte[]... fields);

	/**
	 * Redis Hmset 命令用于同时将多个 field-value (字段-值)对设置到哈希表中。<br/>
	 * 此命令会覆盖哈希表中已存在的字段。<br/>
	 * 如果哈希表不存在，会创建一个空哈希表，并执行 HMSET 操作。
	 * 
	 * @param key
	 *            KEY
	 * @param hash
	 *            多个 field-value (字段-值)对
	 * @return 如果命令执行成功，返回 OK 。
	 */
	String hmset(byte[] key, Map<byte[], byte[]> hash);

	/**
	 * Redis Hset 命令用于为哈希表中的字段赋值 。<br/>
	 * 如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作。<br/>
	 * 如果字段已经存在于哈希表中，旧值将被覆盖。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            字段
	 * @param value
	 *            字段值
	 * @return 如果字段是哈希表中的一个新建字段，并且值设置成功，返回 1 。<br/>
	 *         如果哈希表中域字段已经存在且旧值已被新值覆盖，返回 0 。
	 */
	Long hset(byte[] key, byte[] field, byte[] value);

	/**
	 * Redis Hsetnx 命令用于为哈希表中不存在的的字段赋值 。<br/>
	 * 如果哈希表不存在，一个新的哈希表被创建并进行 HSET 操作。<br/>
	 * 如果字段已经存在于哈希表中，操作无效。<br/>
	 * 如果 key 不存在，一个新哈希表被创建并执行 HSETNX 命令。
	 * 
	 * @param key
	 *            KEY
	 * @param field
	 *            字段
	 * @param value
	 *            字段值
	 * @return 设置成功，返回 1 。 如果给定字段已经存在且没有操作被执行，返回 0 。
	 */
	Long hsetnx(byte[] key, byte[] field, byte[] value);

	/**
	 * Redis Hvals 命令返回哈希表所有字段的值。
	 * 
	 * @param key
	 *            KEY
	 * @return 一个包含哈希表中所有值的表。 当 key 不存在时，返回一个空表。
	 */
	Collection<byte[]> hvals(byte[] key);
}
