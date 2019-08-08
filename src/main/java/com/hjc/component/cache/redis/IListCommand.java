package com.hjc.component.cache.redis;

import java.util.List;

import redis.clients.jedis.Client;

/**
 * Redis 列表(List)命令
 * 
 * @author hjc
 *
 */
public interface IListCommand {
	/**
	 * Redis Lindex 命令用于通过索引获取列表中的元素。<br/>
	 * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * 
	 * @param key
	 *            KEY
	 * @param index
	 *            索引
	 * @return 列表中下标为指定索引值的元素。 如果指定索引值不在列表的区间范围内，返回 nil 。
	 */
	byte[] lindex(byte[] key, long index);

	/**
	 * Redis Linsert 命令用于在列表的元素前或者后插入元素。 <br/>
	 * 当指定元素不存在于列表中时，不执行任何操作。 当列表不存在时，被视为空列表，不执行任何操作。<br/>
	 * 如果 key 不是列表类型，返回一个错误。
	 * 
	 * @param key
	 *            KEY
	 * @param where
	 *            前或者后
	 * @param pivot
	 *            相对位置的元素
	 * @param value
	 *            元素
	 * @return 如果命令执行成功，返回插入操作完成之后，列表的长度。<br/>
	 *         如果没有找到指定元素 ，返回 -1 。 如果 key 不存在或为空列表，返回 0 。
	 */
	Long linsert(byte[] key, Client.LIST_POSITION where, byte[] pivot, byte[] value);

	/**
	 * Redis Llen 命令用于返回列表的长度。 <br/>
	 * 如果列表 key 不存在，则 key 被解释为一个空列表，返回 0 。 <br/>
	 * 如果 key 不是列表类型，返回一个错误。
	 * 
	 * @param key
	 *            KEY
	 * @return 列表的长度。
	 */
	Long llen(byte[] key);

	/**
	 * Redis Lpop 命令用于移除并返回列表的第一个元素。
	 * 
	 * @param key
	 *            KEY
	 * @return 列表的第一个元素。 当列表 key 不存在时，返回 nil。
	 */
	byte[] lpop(byte[] key);

	/**
	 * Redis Lpush 命令将一个或多个值插入到列表头部。<br/>
	 * 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。<br/>
	 * 当 key 存在但不是列表类型时，返回一个错误。
	 * 
	 * @param key
	 *            KEY
	 * @param args
	 *            一个或多个值
	 * @return 执行 LPUSH 命令后，列表的长度。
	 */
	Long lpush(byte[] key, byte[]... args);

	/**
	 * Redis Lpushx 将一个或多个值插入到已存在的列表头部，列表不存在时操作无效。
	 * 
	 * @param key
	 *            KEY
	 * @param arg
	 *            一个或多个值
	 * @return LPUSHX 命令执行之后，列表的长度。
	 */
	Long lpushx(byte[] key, byte[]... args);

	/**
	 * Redis Lrange 返回列表中指定区间内的元素，区间以偏移量 START 和 END 指定。<br/>
	 * 其中 0 表示列表的第一个元素， 1 表示列表的第二个元素，以此类推。<br/>
	 * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * 
	 * @param key
	 *            KEY
	 * @param start
	 *            开始位置
	 * @param end
	 *            结束位置
	 * @return 一个列表，包含指定区间内的元素。
	 */
	List<byte[]> lrange(byte[] key, long start, long end);

	/**
	 * Redis Lrem 根据参数 COUNT 的值，移除列表中与参数 VALUE 相等的元素。<br/>
	 * COUNT 的值可以是以下几种：<br/>
	 * count > 0 : 从表头开始向表尾搜索，移除与 VALUE 相等的元素，数量为 COUNT 。<br/>
	 * count < 0 : 从表尾开始向表头搜索，移除与 VALUE 相等的元素，数量为 COUNT 的绝对值。<br/>
	 * count = 0 : 移除表中所有与 VALUE 相等的值。
	 * 
	 * @param key
	 *            KEY
	 * @param count
	 *            COUNT 值
	 * @param value
	 *            VALUE值
	 * @return 被移除元素的数量。 列表不存在时返回 0
	 */
	Long lrem(byte[] key, long count, byte[] value);

	/**
	 * Redis Lset 通过索引来设置元素的值。<br/>
	 * 当索引参数超出范围，或对一个空列表进行 LSET 时，返回一个错误。<br/>
	 * 关于列表下标的更多信息，请参考 LINDEX 命令。
	 * 
	 * @param key
	 *            KEY
	 * @param index
	 *            索引
	 * @param value
	 *            元素值
	 * @return 操作成功返回 ok ，否则返回错误信息。
	 */
	String lset(byte[] key, long index, byte[] value);

	/**
	 * Redis Ltrim 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。<br/>
	 * 下标 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。 <br/>
	 * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
	 * 
	 * @param key
	 *            KEY
	 * @param start
	 *            开始位置
	 * @param end
	 *            结束位置
	 * @return 命令执行成功时，返回 ok 。
	 */
	String ltrim(byte[] key, long start, long end);

	/**
	 * Redis Rpop 命令用于移除并返回列表的最后一个元素。
	 * 
	 * @param key
	 *            KEY
	 * @return 列表的最后一个元素。 当列表不存在时，返回 nil 。
	 */
	byte[] rpop(byte[] key);

	/**
	 * Redis Rpush 命令用于将一个或多个值插入到列表的尾部(最右边)。<br/>
	 * 如果列表不存在，一个空列表会被创建并执行 RPUSH 操作。 当列表存在但不是列表类型时，返回一个错误。<br/>
	 * 注意：在 Redis 2.4 版本以前的 RPUSH 命令，都只接受单个 value 值。
	 * 
	 * @param key
	 *            KEY
	 * @param args
	 *            一个或多个值
	 * @return 执行 RPUSH 操作后，列表的长度。
	 */
	Long rpush(byte[] key, byte[]... args);

	/**
	 * Redis Rpushx 命令用于将一个或多个值插入到已存在的列表尾部(最右边)。如果列表不存在，操作无效。
	 * 
	 * @param key
	 *            KEY
	 * @param args
	 *            一个或多个值
	 * @return 执行 RPUSH 操作后，列表的长度。
	 */
	Long rpushx(byte[] key, byte[]... args);

	// 不支持的命令：
	// Redis Blpop 命令
	// Redis Brpop 命令
	// Redis Brpoplpush 命令
	// Redis Rpoplpush 命令

}
