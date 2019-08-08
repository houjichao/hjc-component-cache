package com.hjc.component.cache.redis;

import java.io.IOException;

/**
 * redis二进制命令接口
 * 
 * @author hjc
 *
 */
public interface IBinaryJedis
    extends IKeyCommand, IStringCommand, IHashCommand, IListCommand, ISetCommand,
    ISortedSetCommand {
    void close() throws IOException;
}
