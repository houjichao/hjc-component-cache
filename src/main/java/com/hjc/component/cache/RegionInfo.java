package com.hjc.component.cache;

import java.io.Serializable;

import org.apache.commons.lang.builder.ToStringBuilder;

public class RegionInfo implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1884056608293461740L;
	private String namespace;
	private String className;
	private String methodDesc;
	private int expire;
	private boolean refresh;

	public String getNamespace() {
		return namespace;
	}

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getMethodDesc() {
		return methodDesc;
	}

	public void setMethodDesc(String methodDesc) {
		this.methodDesc = methodDesc;
	}

	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	public boolean isRefresh() {
		return refresh;
	}

	public void setRefresh(boolean refresh) {
		this.refresh = refresh;
	}

	@Override
    public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}
}
