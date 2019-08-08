package com.hjc.component.cache;

import java.io.Serializable;
import java.util.List;

/**
 * 分页查询结果
 * 
 * @author hjc
 *
 * @param <T>
 */
public class CacheResult<T> implements Serializable {
	private static final long serialVersionUID = -1866937242431333621L;
	private long total;// 总记录数
	private List<T> list;// 当前页记录(必须支持序列化)

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public List<T> getList() {
		return list;
	}

	public void setList(List<T> list) {
		this.list = list;
	}

}
