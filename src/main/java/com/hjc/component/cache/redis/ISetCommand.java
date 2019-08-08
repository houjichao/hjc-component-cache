package com.hjc.component.cache.redis;

import java.util.Set;

/**
 * Redis 集合(Set)命令
 * 
 * @author hjc
 *
 */
public interface ISetCommand {
	/**
	 * Redis Sadd 命令将一个或多个成员元素加入到集合中，已经存在于集合的成员元素将被忽略。<br/>
	 * 假如集合 key 不存在，则创建一个只包含添加的元素作成员的集合。<br/>
	 * 当集合 key 不是集合类型时，返回一个错误。<br/>
	 * 注意：在Redis2.4版本以前， SADD 只接受单个成员值。
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            元素
	 * @return 被添加到集合中的新元素的数量，不包括被忽略的元素。
	 */
	Long sadd(byte[] key, byte[]... member);

	/**
	 * Redis Scard 命令返回集合中元素的数量。
	 * 
	 * @param key
	 *            KEY
	 * @return 集合的数量。 当集合 key 不存在时，返回 0 。
	 */
	Long scard(byte[] key);

	/**
	 * Redis Sismember 命令判断成员元素是否是集合的成员。
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            元素
	 * @return 如果成员元素是集合的成员，返回 true。<br/>
	 *         如果成员元素不是集合的成员，或 key 不存在，返回 false。
	 */
	Boolean sismember(byte[] key, byte[] member);

	/**
	 * Redis Smembers 命令返回集合中的所有的成员。 不存在的集合 key 被视为空集合。
	 * 
	 * @param key
	 *            KEY
	 * @return 集合中的所有成员。
	 */
	Set<byte[]> smembers(byte[] key);

	/**
	 * Redis Spop 命令用于移除并返回集合中的一个随机元素。
	 * 
	 * @param key
	 *            KEY
	 * @return 被移除的随机元素。 当集合不存在或是空集时，返回 nil 。
	 */
	byte[] spop(byte[] key);

	/**
	 * Redis Srandmember 命令用于返回集合中的一个随机元素。<br/>
	 * 从 Redis 2.6 版本开始， Srandmember 命令接受可选的 count 参数：<br/>
	 * 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。<br/>
	 * 如果 count 大于等于集合基数，那么返回整个集合。<br/>
	 * 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。<br/>
	 * 该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而 Srandmember 则仅仅返回随机元素，而不对集合进行任何改动。
	 * 
	 * @param key
	 *            KEY
	 * @return 只提供集合 key 参数时，返回一个元素；如果集合为空，返回 nil 。 <br/>
	 *         如果提供了 count 参数，那么返回一个数组；如果集合为空，返回空数组。
	 */
	byte[] srandmember(byte[] key);

	/**
	 * Redis Srem 命令用于移除集合中的一个或多个成员元素，不存在的成员元素会被忽略。<br/>
	 * 当 key 不是集合类型，返回一个错误。<br/>
	 * 在 Redis 2.4 版本以前， SREM 只接受单个成员值。
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            元素
	 * @return 被成功移除的元素的数量，不包括被忽略的元素。
	 */
	Long srem(byte[] key, byte[]... member);

	// 不支持的命令：
	// Redis Sdiff 命令
	// Redis Sdiffstore 命令
	// Redis Sinter 命令
	// Redis Sinterstore 命令
	// Redis Smove 命令
	// Redis Sunion 命令
	// Redis Sunionstore 命令
	// Redis Sscan 命令

}
