package com.hjc.component.cache;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 分页查询结果
 * 
 * @author hjc
 *
 * @param <T>
 */
@Data
public class CacheResult<T> implements Serializable {
	private static final long serialVersionUID = -1866937242431333621L;
	/**
	 * 总记录数
	 */
	private long total;
	/**
	 * 当前页记录(必须支持序列化)
	 */
	private List<T> list;
}
