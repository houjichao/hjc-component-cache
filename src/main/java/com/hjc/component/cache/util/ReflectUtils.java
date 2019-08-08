package com.hjc.component.cache.util;

import java.lang.reflect.Method;

import javassist.NotFoundException;

public class ReflectUtils {
	/**
	 * void(V).
	 */
	public static final char JVM_VOID = 'V';

	/**
	 * boolean(Z).
	 */
	public static final char JVM_BOOLEAN = 'Z';

	/**
	 * byte(B).
	 */
	public static final char JVM_BYTE = 'B';

	/**
	 * char(C).
	 */
	public static final char JVM_CHAR = 'C';

	/**
	 * double(D).
	 */
	public static final char JVM_DOUBLE = 'D';

	/**
	 * float(F).
	 */
	public static final char JVM_FLOAT = 'F';

	/**
	 * int(I).
	 */
	public static final char JVM_INT = 'I';

	/**
	 * long(J).
	 */
	public static final char JVM_LONG = 'J';

	/**
	 * short(S).
	 */
	public static final char JVM_SHORT = 'S';

	/**
	 * get class desc. boolean[].class => "[Z" Object.class =>
	 * "Ljava/lang/Object;"
	 * 
	 * @param c
	 *            class.
	 * @return desc.
	 * @throws NotFoundException
	 */
	public static String getDesc(Class<?> c) {
		StringBuilder ret = new StringBuilder();

		while (c.isArray()) {
			ret.append('[');
			c = c.getComponentType();
		}

		if (c.isPrimitive()) {
			String t = c.getName();
			if ("void".equals(t)) {
				ret.append(JVM_VOID);
			} else if ("boolean".equals(t)) {
				ret.append(JVM_BOOLEAN);
			} else if ("byte".equals(t)) {
				ret.append(JVM_BYTE);
			} else if ("char".equals(t)) {
				ret.append(JVM_CHAR);
			} else if ("double".equals(t)) {
				ret.append(JVM_DOUBLE);
			} else if ("float".equals(t)) {
				ret.append(JVM_FLOAT);
			} else if ("int".equals(t)) {
				ret.append(JVM_INT);
			} else if ("long".equals(t)) {
				ret.append(JVM_LONG);
			} else if ("short".equals(t)) {
				ret.append(JVM_SHORT);
			}
		} else {
			String clazzName = c.getName();

			ret.append('L');
			ret.append(clazzName.replace('.', '/'));
			ret.append(';');
		}
		return ret.toString();
	}

	/**
	 * get method desc. int do(int arg1) => "do(I)I" void do(String arg1,boolean
	 * arg2) => "do(Ljava/lang/String;Z)V"
	 * 
	 * @param m
	 *            method.
	 * @return desc.
	 */
	public static String getDesc(final Method m) {
		StringBuilder ret = new StringBuilder(m.getName()).append('(');
		Class<?>[] parameterTypes = m.getParameterTypes();
		for (int i = 0; i < parameterTypes.length; i++) {
			ret.append(getDesc(parameterTypes[i]));
		}
		ret.append(')').append(getDesc(m.getReturnType()));
		return ret.toString();
	}

	/**
	 * 获取函数签名
	 * 
	 * @param methodName
	 *            方法名
	 * @param parameterTypes
	 *            参数类型
	 * @param returnType
	 *            返回类型
	 * @return 签名字符串
	 */
	public static String getSignature(String methodName, Class<?>[] parameterTypes, Class<?> returnType) {
		StringBuilder ret = new StringBuilder(methodName).append('(');
		if (parameterTypes != null) {
			for (int i = 0; i < parameterTypes.length; i++) {
				ret.append(getDesc(parameterTypes[i]));
			}
		}
		ret.append(')').append(getDesc(returnType));
		return ret.toString();
	}
}
