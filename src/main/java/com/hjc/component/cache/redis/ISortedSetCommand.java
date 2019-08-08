package com.hjc.component.cache.redis;

import java.util.Map;
import java.util.Set;

/**
 * Redis 有序集合(sorted set)命令
 * 
 * @author hjc
 *
 */
public interface ISortedSetCommand {
	/**
	 * Redis Zadd 命令用于将一个或多个成员元素及其分数值加入到有序集当中。<br/>
	 * 如果某个成员已经是有序集的成员，那么更新这个成员的分数值，并通过重新插入这个成员元素，来保证该成员在正确的位置上。<br/>
	 * 分数值可以是整数值或双精度浮点数。<br/>
	 * 如果有序集合 key 不存在，则创建一个空的有序集并执行 ZADD 操作。<br/>
	 * 当 key 存在但不是有序集类型时，返回一个错误。<br/>
	 * 注意： 在 Redis 2.4 版本以前， ZADD 每次只能添加一个元素。
	 * 
	 * @param key
	 *            KEY
	 * @param score
	 *            分数值
	 * @param member
	 *            成员元素
	 * @return 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
	 */
	Long zadd(byte[] key, double score, byte[] member);

	/**
	 * Redis Zadd 命令用于将一个或多个成员元素及其分数值加入到有序集当中。<br/>
	 * 如果某个成员已经是有序集的成员，那么更新这个成员的分数值，并通过重新插入这个成员元素，来保证该成员在正确的位置上。<br/>
	 * 分数值可以是整数值或双精度浮点数。<br/>
	 * 如果有序集合 key 不存在，则创建一个空的有序集并执行 ZADD 操作。<br/>
	 * 当 key 存在但不是有序集类型时，返回一个错误。<br/>
	 * 注意： 在 Redis 2.4 版本以前， ZADD 每次只能添加一个元素。
	 * 
	 * @param key
	 *            KEY
	 * @param scoreMembers
	 *            成员元素——分数值
	 * @return 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
	 */
	Long zadd(byte[] key, Map<byte[], Double> scoreMembers);

	/**
	 * Redis Zcard 命令用于计算集合中元素的数量。
	 * 
	 * @param key
	 *            KEY
	 * @return 当 key 存在且是有序集类型时，返回有序集的基数。 当 key 不存在时，返回 0 。
	 */
	Long zcard(byte[] key);

	/**
	 * Redis Zcount 命令用于计算有序集合中指定分数区间的成员数量。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 分数值在 min 和 max 之间的成员的数量。
	 */
	Long zcount(byte[] key, double min, double max);

	/**
	 * Redis Zincrby 命令对有序集合中指定成员的分数加上增量 increment<br/>
	 * 可以通过传递一个负数值 increment ，让分数减去相应的值，<br/>
	 * 比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。<br/>
	 * 当 key 不存在，或分数不是 key 的成员时， <br/>
	 * ZINCRBY key increment member 等同于 ZADD key increment member 。<br/>
	 * 当 key 不是有序集类型时，返回一个错误。<br/>
	 * 分数值可以是整数值或双精度浮点数。
	 * 
	 * @param key
	 *            KEY
	 * @param score
	 *            分数值
	 * @param member
	 *            成员元素
	 * @return 成员的新分数值
	 */
	Double zincrby(byte[] key, double score, byte[] member);

	/**
	 * Redis Zlexcount 命令在计算有序集合中指定字典区间内成员数量。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 指定区间内的成员数量。
	 */
	Long zlexcount(final byte[] key, final byte[] min, final byte[] max);

	/**
	 * Redis Zrange 返回有序集中，指定区间内的成员。<br/>
	 * 其中成员的位置按分数值递增(从小到大)来排序。<br/>
	 * 具有相同分数值的成员按字典序(lexicographical order )来排列。<br/>
	 * 如果你需要成员按值递减(从大到小)来排列，请使用 ZREVRANGE 命令。<br/>
	 * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。<br/>
	 * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推
	 * 
	 * @param key
	 *            KEY
	 * @param start
	 *            起始位置
	 * @param end
	 *            结束位置
	 * @return 指定区间内，带有分数值(可选)的有序集成员的列表。
	 */
	Set<byte[]> zrange(byte[] key, long start, long end);

	/**
	 * Redis Zrangebylex 通过字典区间返回有序集合的成员。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 指定区间内的元素列表。
	 */
	Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max);

	/**
	 * Redis Zrangebylex 通过字典区间返回有序集合的成员。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @param offset
	 *            偏移位
	 * @param count
	 *            最大数
	 * @return 指定区间内的元素列表。
	 */
	Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max, int offset, int count);

	/**
	 * Redis Zrangebyscore 返回有序集合中指定分数区间的成员列表。有序集成员按分数值递增(从小到大)次序排列。<br/>
	 * 具有相同分数值的成员按字典序来排列(该属性是有序集提供的，不需要额外的计算)。<br/>
	 * 默认情况下，区间的取值使用闭区间 (小于等于或大于等于)，你也可以通过给参数前增加 ( 符号来使用可选的开区间 (小于或大于)。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 指定区间内，带有分数值(可选)的有序集成员的列表。
	 */
	Set<byte[]> zrangeByScore(byte[] key, double min, double max);

	/**
	 * Redis Zrank 返回有序集中指定成员的排名。其中有序集成员按分数值递增(从小到大)顺序排列。
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            成员元素
	 * @return 如果成员是有序集 key 的成员，返回 member 的排名。 如果成员不是有序集 key 的成员，返回 nil 。
	 */
	Long zrank(byte[] key, byte[] member);

	/**
	 * Redis Zrem 命令用于移除有序集中的一个或多个成员，不存在的成员将被忽略。<br/>
	 * 当 key 存在但不是有序集类型时，返回一个错误。<br/>
	 * 注意： 在 Redis 2.4 版本以前， ZREM 每次只能删除一个元素。
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            成员元素
	 * @return 被成功移除的成员的数量，不包括被忽略的成员。
	 */
	Long zrem(byte[] key, byte[]... member);

	/**
	 * Redis Zremrangebylex 命令用于移除有序集合中给定的字典区间的所有成员。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 被成功移除的成员的数量，不包括被忽略的成员。
	 */
	Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max);

	/**
	 * Redis Zremrangebyrank 命令用于移除有序集中，指定排名(rank)区间内的所有成员。
	 * 
	 * @param key
	 *            KEY
	 * @param start
	 *            起始位置
	 * @param end
	 *            结束位置
	 * @return 被移除成员的数量。
	 */
	Long zremrangeByRank(byte[] key, long start, long end);

	/**
	 * Redis Zremrangebyscore 命令用于移除有序集中，指定分数（score）区间内的所有成员。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 被移除成员的数量。
	 */
	Long zremrangeByScore(byte[] key, double min, double max);

	/**
	 * Redis Zrevrange 命令返回有序集中，指定区间内的成员。<br/>
	 * 其中成员的位置按分数值递减(从大到小)来排列。<br/>
	 * 具有相同分数值的成员按字典序的逆序(reverse lexicographical order)排列。<br/>
	 * 除了成员按分数值递减的次序排列这一点外， ZREVRANGE 命令的其他方面和 ZRANGE 命令一样。
	 * 
	 * @param key
	 *            KEY
	 * @param start
	 *            起始位置
	 * @param end
	 *            结束位置
	 * @return 指定区间内，带有分数值(可选)的有序集成员的列表。
	 */
	Set<byte[]> zrevrange(byte[] key, long start, long end);

	/**
	 * Redis Zremrangebyscore 命令用于移除有序集中，指定分数（score）区间内的所有成员。
	 * 
	 * @param key
	 *            KEY
	 * @param min
	 *            最小分值
	 * @param max
	 *            最大分值
	 * @return 被移除成员的数量。
	 */
	Set<byte[]> zrevrangeByScore(byte[] key, double max, double min);

	/**
	 * Redis Zrevrank 命令返回有序集中成员的排名。其中有序集成员按分数值递减(从大到小)排序。<br/>
	 * 排名以 0 为底，也就是说， 分数值最大的成员排名为 0 。<br/>
	 * 使用 ZRANK 命令可以获得成员按分数值递增(从小到大)排列的排名。<br/>
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            成员元素
	 * @return 如果成员是有序集 key 的成员，返回成员的排名。 如果成员不是有序集 key 的成员，返回 nil。
	 */
	Long zrevrank(byte[] key, byte[] member);

	/**
	 * Redis Zscore 命令返回有序集中，成员的分数值。 如果成员元素不是有序集 key 的成员，或 key 不存在，返回 nil 。
	 * 
	 * @param key
	 *            KEY
	 * @param member
	 *            成员元素
	 * @return 成员的分数值
	 */
	Double zscore(byte[] key, byte[] member);

	// 不支持的命令：
	// Redis Zinterstore 命令
	// Redis Zunionstore 命令
	// Redis Zscan 命令

}
