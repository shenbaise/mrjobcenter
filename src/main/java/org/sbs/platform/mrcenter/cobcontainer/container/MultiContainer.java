/**
 * @工程 mr job center
 * @文件 MultiContainer.java
 * @时间 2013年9月10日 下午1:50:46
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.Collection;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc Multimap实现的多值容器
 */
public abstract class MultiContainer<T, K> {
	
	Multimap<T,K> container = ArrayListMultimap.create();
	
	/**
	 * 实现该方法，对容器进行初始化
	 */
	public abstract void init();
	
	public Collection<K> getContent(T t){
		
		return container.get(t);
	}
	
	public synchronized Collection<K> getContentThenRemove(T t){
		Collection<K> k = container.get(t);
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
		container.removeAll(t);
	}
	public synchronized void remove(T t,K k){
		container.remove(t,k);
	}
	public synchronized void destroy(){
		container.clear();
		container = null;
	}
	public synchronized void reInit(){
		container.clear();
		init();
	}
}
