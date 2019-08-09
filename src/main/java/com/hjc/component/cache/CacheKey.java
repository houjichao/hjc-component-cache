package com.hjc.component.cache;

import com.hjc.component.cache.util.Serialize;
import com.hjc.component.cache.util.StringHelper;

import java.io.Serializable;


public class CacheKey implements Serializable {
	private static final long serialVersionUID = -7299223069008165363L;
	/**
	 * 为了防止不同应用Key冲突
	 */
	private String namespace;
	/**
	 * 缓存Key
	 */
	private String key;
	/**
	 * 哈希的field(如何设置此项，则使用hash存储)
	 */
	private String hfield;

	public static CacheKey from(byte[] fullKeyBytes) {
		String[] args = Serialize.fstdeserialize(fullKeyBytes);
		CacheKey key = new CacheKey();
		key.setNamespace(args[0]);
		key.setKey(args[1]);
		key.setHfield(args[2]);
		return key;
	}

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getHfield() {
		return hfield;
	}

	public void setHfield(String hfield) {
		this.hfield = hfield;
	}

	public String getCacheKey() {
		if (StringHelper.isNotEmpty(this.namespace)) {
			return this.namespace + ":" + this.key;
		}
		return this.key;
	}

	public String getFullKey() {
		StringBuilder b = new StringBuilder();
		if (StringHelper.isNotEmpty(this.namespace)) {
			b.append(this.namespace).append(":");
		}
		b.append(this.key);
		if (StringHelper.isNotEmpty(this.hfield)) {
			b.append(":").append(this.hfield);
		}
		return b.toString();
	}

	public byte[] getFullKeyBytes() {
		return Serialize.fstserialize(new String[] { namespace, key, hfield });
	}

}
