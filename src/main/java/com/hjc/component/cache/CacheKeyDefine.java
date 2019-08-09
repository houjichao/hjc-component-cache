package com.hjc.component.cache;

/**
 * 底层缓存KEY定义
 * 
 * @author hjc
 *
 */
public interface CacheKeyDefine {
	/**
	 * 分->秒数
	 */
	int MINUTE = 60;
	/**
	 * 时->秒数
	 */
	int HOUR = 60 * MINUTE;
	/**
	 * 天->秒数
	 */
	int DAY = 24 * HOUR;

	/////////////////

	/**
	 * 信息项OsInfoEntity的缓存KEY args[0]=信息项ID<br />
	 * 'OSINFO@{infoId='+#args[0]+'}'
	 */
	String OSINFO_KEY = "'OSINFO@{infoId='+#args[0]+'}'";
	/**
	 * 信息项OsInfoEntity的缓存时间
	 */
	int OSINFO_EXPIRE = 1 * DAY;
	/**
	 * 表达式的缓存Key args[0]=表达式ID<br/>
	 * 'OSEXPR@{expId='+#args[0]+'}'
	 */
	String OSEXPR_KEY = "'OSEXPR@{expId='+#args[0]+'}'";
	/**
	 * 表达式的缓存时间
	 */
	int OSEXPR_EXPIRE = 1 * HOUR;
	/**
	 * 字典值的缓存Key args[0]=DICT_TYPEID,args[1]=DICT_ID,args[2]=CITY_ID<br/>
	 * 'PCDICT@{dictTypeid='+#args[0]+',dictId='+#args[1]+',cityId='+#args[2]+'}
	 * '
	 */
	String PCDICT_KEY = "'PCDICT@{dictTypeid='+#args[0]+',dictId='+#args[1]+',cityId='+#args[2]+'}'";
	/**
	 * 字典值的缓存时间
	 */
	int PCDICT_EXPIRE = 1 * DAY;

	/**
	 * ESB服务接入工号的缓存Key args[0]=STAFF_ID<br/>
	 * 'WSSTAFF@{staffId='+#args[0]+'}'
	 */
	String WSSTAFF_KEY = "'WSSTAFF@{staffId='+#args[0]+'}'";
	/**
	 * ESB服务接入工号的缓存时间
	 */
	int WSSTAFF_EXPIRE = 1 * DAY;
	/**
	 * 服务配置的缓存Key args[0]=SYS_ID,args[1]=BUS_CODE,args[2]=SERVICE_CODE<br/>
	 * 'WSSERVIE@{sysId='+#args[0]+',busCode='+#args[1]+',serviceCode='+#args[2]
	 * + '}'
	 */
	String WSSERVIE_KEY = "'WSSERVIE@{sysId='+#args[0]+',busCode='+#args[1]+',serviceCode='+#args[2] + '}'";
	/**
	 * 服务配置的缓存时间
	 */
	int WSSERVIE_EXPIRE = 1 * DAY;
	/**
	 * 服务权限的缓存Key args[0]=SRC_SYS_ID,args[1]=BUS_CODE,args[2]=DST_SYS_ID<br/>
	 * 'WSAUTH@{srcSysId='+#args[0]+',busCode='+#args[1]+',dstSysId'+#args[2]+'}
	 * '
	 */
	String WSAUTH_KEY = "'WSAUTH@{srcSysId='+#args[0]+',busCode='+#args[1]+',dstSysId'+#args[2]+'}'";
	/**
	 * 服务权限的缓存时间
	 */
	int WSAUTH_EXPIRE = 1 * DAY;
	/**
	 * 服务地址规则的缓存Key args[0]=SYS_ID,args[1]=BUS_CODE,args[2]=CITYID<br/>
	 * 'WSADDR@{sysId='+#args[0]+',busCode='+#args[1]+',cityId='+#args[2]+'}'
	 */
	String WSADDR_KEY = "'WSADDR@{sysId='+#args[0]+',busCode='+#args[1]+',cityId='+#args[2]+'}'";
	/**
	 * 服务地址规则的缓存时间
	 */
	int WSADDR_EXPIRE = 1 * DAY;

	/**
	 * 转换异常编码的缓存Key args[0]=VERSION,args[1]=EXPCODE<br/>
	 * 'PCERRTRANS@'+#args[0]+'@'+#args[1]
	 */
	String PCERRTRANS_KEY = "'PCERRTRANS@{version='+#args[0]+',expcode='+#args[1]+'}'";
	/**
	 * 转换异常编码的缓存时间
	 */
	int PCERRTRANS_EXPIRE = 1 * DAY;

	/////////////////

	/**
	 * JCS的region定义
	 */
	String JCS_CRMPUB_REGION = "hjcpub";
}
