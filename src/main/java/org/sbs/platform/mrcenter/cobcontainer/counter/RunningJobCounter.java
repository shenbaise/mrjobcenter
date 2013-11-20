/**
 * @工程 mr job center
 * @文件 RunningJobCounter.java
 * @时间 2013年9月9日 上午9:59:59
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.counter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc
 */
public class RunningJobCounter {
	
	private static AtomicInteger counter = new AtomicInteger(0);
	
	private static int max = 5;
	
	public static int increment(){
		return counter.incrementAndGet();
	}
	
	public static int decrement(){
		return counter.decrementAndGet();
	}
	
}
