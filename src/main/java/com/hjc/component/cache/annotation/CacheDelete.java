package com.hjc.component.cache.annotation;

import com.hjc.component.cache.CacheType;
import com.hjc.component.cache.util.StringHelper;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Inherited
@Documented
public @interface CacheDelete {
	/**
	 * 缓存类型
	 * 
	 * @return CacheType
	 */
	CacheType type() default CacheType.REDIS;

	/**
	 * KEY所属区域(JCS必填)
	 * 
	 * @return String
	 */
	String region() default StringHelper.EMPTY;

	/**
	 * 缓存的条件，可以为空，使用 SpEL 编写，返回 true 或者 false，只有为 true 才进行缓存
	 * 
	 * @return String
	 */
	String condition() default StringHelper.EMPTY;

	/**
	 * 自定义缓存Key，支持Spring EL表达式
	 * 
	 * @return String 自定义缓存Key
	 */
	String key();

	/**
	 * 自定义Hash缓存的field，支持Spring EL表达式
	 * 
	 * @return String 自定义Hash缓存的field
	 */
	String hfield() default StringHelper.EMPTY;

}
