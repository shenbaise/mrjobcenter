/**
 * @工程 easoustat_0.0.4
 * @文件 MrClassLoader.java
 * @时间 2013年10月8日 下午2:37:29
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.classloader;

import java.io.File;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc mr程序加载器。
 */
public class MrClassLoader {
	// 插件目录
	private static String pluginDir = "plugin";
	// Class loader
	private static CommonClassLoader classLoader = null;
	
	/**
	 * 初始化
	 * @throws Exception 
	 */
	public static void init() throws Exception{
		if(classLoader==null){
			classLoader = new CommonClassLoader(Thread.currentThread().getContextClassLoader());
			File fPluginDir = new File(pluginDir);
			if(!fPluginDir.exists()){
				fPluginDir.mkdir();
			}
			File[] fs = fPluginDir.listFiles();
			classLoader.addEntries(fs);
			Thread.currentThread().setContextClassLoader(classLoader);
		}else {
			throw new Exception("类加载器已经初始化，不可重复初始化！");
		}
	}
	
	public static synchronized void addPlugin(File f){
		classLoader.addEntry(f);
	}
	
	public static synchronized void removePlugin(File f){
		classLoader.removeEntry(f);
	}
	
	/**
	 * 加载类
	 * @param className
	 * @return 
	 */
	public static Class loadClass(String className){
		try {
			return classLoader.loadClass(className, true);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
