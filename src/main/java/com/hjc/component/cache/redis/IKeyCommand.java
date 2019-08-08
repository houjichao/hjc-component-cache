package com.hjc.component.cache.redis;

/**
 * Redis 键(key) 命令
 * 
 * @author hjc
 *
 */
public interface IKeyCommand {
	/**
	 * Redis DEL 命令用于删除已存在的键。不存在的 key 会被忽略。
	 * 
	 * @param key
	 *            KEY
	 * @return 被删除 key 的数量。
	 */
	Long del(final byte[] key);

	/**
	 * Redis EXISTS 命令用于检查给定 key 是否存在。
	 * 
	 * @param key
	 *            KEY
	 * @return 若 key 存在返回 true ，否则返回false。
	 */
	Boolean exists(byte[] key);

	/**
	 * Redis Expire 命令用于设置 key 的过期时间。key 过期后将不再可用。
	 * 
	 * @param key
	 *            KEY
	 * @param seconds
	 *            有效时间（单位：秒）
	 * @return 设置成功返回 1 。 当 key 不存在或者不能为 key 设置过期时间时(比如在低于 2.1.3 版本的 Redis
	 *         中你尝试更新 key 的过期时间)返回 0
	 */
	Long expire(byte[] key, int seconds);

	/**
	 * Redis Expireat 命令用于以 UNIX 时间戳(unix timestamp)格式设置 key 的过期时间。key 过期后将不再可用。
	 * 
	 * @param key
	 *            KEY
	 * @param unixTime
	 *            UNIX 时间戳
	 * @return 设置成功返回 1 。 当 key 不存在或者不能为 key 设置过期时间时(比如在低于 2.1.3 版本的 Redis
	 *         中你尝试更新 key 的过期时间)返回 0 。
	 */
	Long expireAt(byte[] key, long unixTime);

	/**
	 * Redis PEXPIRE 命令用于设置 key 的过期时间，以毫秒计。key 过期后将不再可用。
	 * 
	 * @param key
	 *            KEY
	 * @param milliseconds
	 *            毫秒数
	 * @return 设置成功返回 1 。 当 key 不存在或者不能为 key 设置过期时间时(比如在低于 2.1.3 版本的 Redis
	 *         中你尝试更新 key 的过期时间)返回 0 。
	 */
	Long pexpire(byte[] key, long milliseconds);

	/**
	 * Redis PEXPIREAT 命令用于设置 key 的过期时间，以毫秒计。key 过期后将不再可用。
	 * 
	 * @param key
	 *            KEY
	 * @param millisecondsTimestamp
	 *            UNIX 时间戳(毫秒级)
	 * @return 设置成功返回 1 。 当 key 不存在或者不能为 key 设置过期时间时(比如在低于 2.1.3 版本的 Redis
	 *         中你尝试更新 key 的过期时间)返回 0
	 */
	Long pexpireAt(byte[] key, long millisecondsTimestamp);

	/**
	 * Redis PERSIST 命令用于移除给定 key 的过期时间，使得 key 永不过期。
	 * 
	 * @param key
	 *            KEY
	 * @return 当过期时间移除成功时，返回 1 。 如果 key 不存在或 key 没有设置过期时间，返回 0 。
	 */
	Long persist(byte[] key);

	/**
	 * Redis TTL 命令以秒为单位返回 key 的剩余过期时间。
	 * 
	 * @param key
	 *            KEY
	 * @return 当 key 不存在时，返回 -2 。 当 key 存在但没有设置剩余生存时间时，返回 -1 。 否则，以毫秒为单位，返回 key
	 *         的剩余生存时间。
	 */
	Long ttl(byte[] key);

	/**
	 * Redis Type 命令用于返回 key 所储存的值的类型。
	 * 
	 * @param key
	 *            KEY
	 * @return 返回 key 的数据类型，数据类型有： none (key不存在) string (字符串) list (列表) set (集合)
	 *         zset (有序集) hash (哈希表)
	 * 
	 */
	String type(byte[] key);

	// 不支持的命令：
	// Redis Dump 命令 序列化给定 key ，并返回被序列化的值。
	// Redis Keys 命令 查找所有符合给定模式( pattern)的 key 。
	// Redis Move 命令 将当前数据库的 key 移动到给定的数据库 db 当中。
	// Redis Pttl 命令 以毫秒为单位返回 key 的剩余的过期时间。
	// Redis RANDOMKEY 命令 从当前数据库中随机返回一个 key 。
	// Redis Rename 命令 修改 key 的名称
	// Redis Renamenx 命令 仅当 newkey 不存在时，将 key 改名为 newkey 。
}
