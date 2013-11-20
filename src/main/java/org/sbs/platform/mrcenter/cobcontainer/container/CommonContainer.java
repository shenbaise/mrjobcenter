/**
 * @工程 mr job center
 * @文件 Container.java
 * @时间 2013年9月9日 上午10:56:29
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc 通用的容器。
 */
public abstract class CommonContainer<T,K> {
	
	Log log = LogFactory.getLog(CommonContainer.class);
	
	private TreeMap<T, K> container = new TreeMap<T, K>();
	
	protected CommonContainer(){
		init();
	}
	
	/**
	 * 实现该方法，对容器进行初始化
	 */
	public abstract boolean init();
	
	public synchronized K getContent(T t){
		return container.get(t);
	}
	
	public synchronized K getContentThenRemove(T t){
		K k = container.get(t);
		remove(t);
		return k;
	}
	
	public synchronized void addContent(T t,K k){
		container.put(t, k);
	}
	public synchronized void updateContent(T t,K k){
		container.put(t,k);
	}
	public synchronized void remove(T t){
		container.remove(t);
	}
	public synchronized void destroy(){
		container.clear();
		container = null;
	}
	public synchronized void reInit(){
		container.clear();
		init();
	}
	
	public TreeMap<T,K> getContainer(){
		return container;
	}
}
