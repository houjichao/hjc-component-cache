package com.hjc.component.cache.redis;

/**
 * Redis 字符串(String)命令
 * 
 * @author hjc
 *
 */
public interface IStringCommand {
	/**
	 * Redis SET 命令用于设置给定 key 的值。如果 key 已经存储其他值， SET 就覆写旧值，且无视类型。
	 * 
	 * @param key
	 *            KEY
	 * @param value
	 *            VALUE
	 * @return 在 Redis 2.6.12 以前版本， SET 命令总是返回 OK 。 <br/>
	 *         从 Redis 2.6.12 版本开始， SET 在设置操作成功完成时，才返回 OK 。
	 */
	String set(byte[] key, byte[] value);

	/**
	 * Redis Get 命令用于获取指定 key 的值。如果 key 不存在，返回 nil 。如果key 储存的值不是字符串类型，返回一个错误
	 * 
	 * @param key
	 *            KEY
	 * @return 返回 key 的值，如果 key 不存在时，返回 nil。 如果 key 不是字符串类型，那么返回一个错误。
	 */
	byte[] get(byte[] key);

	/**
	 * Redis Getrange 命令用于获取存储在指定 key 中字符串的子字符串。<br/>
	 * 字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)。
	 * 
	 * @param key
	 *            KEY
	 * @param startOffset
	 *            起始位置
	 * @param endOffset
	 *            结束位置
	 * @return 截取得到的子字符串。
	 */
	byte[] getrange(byte[] key, long startOffset, long endOffset);

	/**
	 * Redis Getset 命令用于设置指定 key 的值，并返回 key 旧的值。
	 * 
	 * @param key
	 *            KEY
	 * @param value
	 *            VALUE
	 * @return 返回给定 key 的旧值。 当 key 没有旧值时，即 key 不存在时，返回 nil 。<br/>
	 *         当 key 存在但不是字符串类型时，返回一个错误。
	 */
	byte[] getSet(byte[] key, byte[] value);

	/**
	 * Redis Getbit 命令用于对 key 所储存的字符串值，获取指定偏移量上的位(bit)。
	 * 
	 * @param key
	 *            KEY
	 * @param offset
	 *            偏移量
	 * @return 字符串值指定偏移量上的位(bit)。<br/>
	 *         当偏移量 OFFSET 比字符串值的长度大，或者 key 不存在时，返回 0 。
	 */
	Boolean getbit(byte[] key, long offset);

	/**
	 * Redis Setbit 命令用于对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
	 * 
	 * @param key
	 *            KEY
	 * @param offset
	 *            偏移量
	 * @param value
	 *            VALUE
	 * @return 指定偏移量原来储存的位。
	 */
	Boolean setbit(byte[] key, long offset, boolean value);

	/**
	 * Redis Setex 命令为指定的 key 设置值及其过期时间。如果 key 已经存在， SETEX 命令将会替换旧的值。
	 * 
	 * @param key
	 *            KEY
	 * @param seconds
	 *            过期时间（单位：秒）
	 * @param value
	 *            VALUE
	 * @return 设置成功时返回 OK 。
	 */
	String setex(byte[] key, int seconds, byte[] value);

	/**
	 * Redis Setnx（SET if Not eXists） 命令在指定的 key 不存在时，为 key 设置指定的值。
	 * 
	 * @param key
	 *            KEY
	 * @param value
	 *            VALUE
	 * @return 设置成功，返回 1 。 设置失败，返回 0 。
	 */
	Long setnx(byte[] key, byte[] value);

	/**
	 * Redis Setrange 命令用指定的字符串覆盖给定 key 所储存的字符串值，覆盖的位置从偏移量 offset 开始。
	 * 
	 * @param key
	 *            KEY
	 * @param offset
	 *            偏移量
	 * @param value
	 *            VALUE
	 * @return 被修改后的字符串长度。
	 */
	Long setrange(byte[] key, long offset, byte[] value);

	/**
	 * Redis Strlen 命令用于获取指定 key 所储存的字符串值的长度。当 key 储存的不是字符串值时，返回一个错误。
	 * 
	 * @param key
	 *            KEY
	 * @return 字符串值的长度。 当 key 不存在时，返回 0。
	 */
	Long strlen(byte[] key);

	/**
	 * Redis Incr 命令将 key 中储存的数字值增一。<br/>
	 * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。<br/>
	 * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。<br/>
	 * 本操作的值限制在 64 位(bit)有符号数字表示之内。
	 * 
	 * @param key
	 *            KEY
	 * @return 执行 INCR 命令之后 key 的值。
	 */
	Long incr(byte[] key);

	/**
	 * Redis Incrby 命令将 key 中储存的数字加上指定的增量值。<br/>
	 * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY 命令。<br/>
	 * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。<br/>
	 * 本操作的值限制在 64 位(bit)有符号数字表示之内。
	 * 
	 * @param key
	 *            KEY
	 * @param integer
	 *            增量值
	 * @return 加上指定的增量值之后， key 的值。
	 */
	Long incrBy(byte[] key, long integer);

	/**
	 * Redis Incrbyfloat 命令为 key 中所储存的值加上指定的浮点数增量值。<br/>
	 * 如果 key 不存在，那么 INCRBYFLOAT 会先将 key 的值设为 0 ，再执行加法操作。
	 * 
	 * @param key
	 *            KEY
	 * @param value
	 *            浮点数增量值
	 * @return 执行命令之后 key 的值。
	 */
	Double incrByFloat(byte[] key, double value);

	/**
	 * Redis Decr 命令将 key 中储存的数字值减一。<br/>
	 * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。<br/>
	 * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。<br/>
	 * 本操作的值限制在 64 位(bit)有符号数字表示之内。
	 * 
	 * @param key
	 *            KEY
	 * @return 执行命令之后 key 的值。
	 */
	Long decr(byte[] key);

	/**
	 * Redis Decrby 命令将 key 所储存的值减去指定的减量值。<br/>
	 * 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY 操作。<br/>
	 * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。<br/>
	 * 本操作的值限制在 64 位(bit)有符号数字表示之内。
	 * 
	 * @param key
	 *            KEY
	 * @param integer
	 *            减量值
	 * @return 执行命令之后 key 的值。
	 */
	Long decrBy(byte[] key, long integer);

	/**
	 * Redis Append 命令用于为指定的 key 追加值。<br/>
	 * 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。<br/>
	 * 如果 key 不存在， APPEND 就简单地将给定 key 设为 value ，就像执行 SET key value 一样。<br/>
	 * 
	 * @param key
	 *            KEY
	 * @param value
	 *            追加值
	 * @return 追加指定值之后， key 中字符串的长度。
	 */
	Long append(byte[] key, byte[] value);

	// 不支持的命令：
	// Redis Mget 命令 获取所有(一个或多个)给定 key 的值。
	// Redis Mset 命令 同时设置一个或多个 key-value 对。
	// Redis Msetnx 命令 同时设置一个或多个 key-value 对，当且仅当所有给定 key 都不存在。
	// Redis Psetex 命令 这个命令和 SETEX 命令相似，但它以毫秒为单位设置 key 的生存时间，而不是像 SETEX
	// 命令那样，以秒为单位。

}
