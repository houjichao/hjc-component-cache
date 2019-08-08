package com.hjc.component.cache.util;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class BeanUtil {
	private static final Map<Class<?>, Field[]> fieldsCahce = new ConcurrentHashMap<Class<?>, Field[]>();

	/**
	 * 是否为基础数据类型
	 * 
	 * @param obj
	 *            Object
	 * @return boolean true or false
	 */
	private static boolean isPrimitive(Object obj) {
		return obj.getClass().isPrimitive() || obj instanceof String || obj instanceof Integer || obj instanceof Long
				|| obj instanceof Byte || obj instanceof Character || obj instanceof Boolean || obj instanceof Short
				|| obj instanceof Float || obj instanceof Double || obj instanceof BigDecimal;
	}

	/**
	 * 把Bean转换为字符串
	 * 
	 * @param obj
	 *            Object
	 * @return String String
	 */
	public static String toString(Object obj) {
		if (obj == null) {
			return "null";
		}

		Class<?> cl = obj.getClass();
		if (isPrimitive(obj)) {
			return String.valueOf(obj);
		} else if (obj instanceof Enum) {
			return ((Enum<?>) obj).name();
		} else if (obj instanceof Date) {
			return String.valueOf(((Date) obj).getTime());
		} else if (obj instanceof Calendar) {
			return String.valueOf(((Calendar) obj).getTime().getTime());
		} else if (cl.isArray()) {
			StringBuilder out = new StringBuilder();
			int len = Array.getLength(obj);
			for (int i = 0; i < len; i++) {
				if (out.length() > 0) {
					out.append(",");
				}
				Object val = Array.get(obj, i);
				out.append(toString(val));
			}
			return "[" + out + "]";
		} else if (obj instanceof Collection) {
			Collection<?> tempCol = (Collection<?>) obj;
			StringBuilder out = new StringBuilder();
			for (Object val : tempCol) {
				if (out.length() > 0) {
					out.append(",");
				}
				out.append(toString(val));
			}
			return "[" + out + "]";
		} else if (obj instanceof Map) {
			Map<?, ?> tempMap = (Map<?, ?>) obj;
			StringBuilder out = new StringBuilder();
			for (Map.Entry<?, ?> entry : tempMap.entrySet()) {
				if (out.length() > 0) {
					out.append(",");
				}
				Object key = entry.getKey();
				out.append(toString(key));
				out.append("=");
				Object val = entry.getValue();
				out.append(toString(val));
			}
			return "{" + out + "}";
		} else if (obj instanceof Class) {
			Class<?> tmpCls = (Class<?>) obj;
			return tmpCls.getName();
		}

		String r = cl.getName();
		do {
			Field[] fields = fieldsCahce.get(cl);
			if (null == fields) {
				fields = cl.getDeclaredFields();
				if (null != fields) {
					AccessibleObject.setAccessible(fields, true);
				}
				fieldsCahce.put(cl, fields);
			}
			if (null == fields || fields.length == 0) {
				cl = cl.getSuperclass();
				continue;
			}
			r += "{";
			// get the names and values of all fields
			for (Field f : fields) {
				if (Modifier.isStatic(f.getModifiers())) {
					continue;
				}
				if (f.isSynthetic() || f.getName().indexOf("this$") != -1) {
					continue;
				}
				r += f.getName() + "=";
				try {
					Object val = f.get(obj);
					r += toString(val);
				} catch (Exception e) {
					e.printStackTrace();
				}
				r += ",";

			}
			if (r.endsWith(",")) {
				r = r.substring(0, r.length() - 1);
			}
			r += "}";
			cl = cl.getSuperclass();
		} while (cl != null);
		return r;
	}

}
