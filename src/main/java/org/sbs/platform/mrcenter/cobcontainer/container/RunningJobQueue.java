/**
 * @工程 mr job center
 * @文件 RunningJobQueue.java
 * @时间 2013年9月9日 上午10:08:10
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.sbs.platform.mrcenter.cobcontainer.job.MrJob;
import org.sbs.platform.mrcenter.cobcontainer.utils.Config;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc 正在运行的job队列。
 */
public class RunningJobQueue {
	
	
	private static BlockingQueue<MrJob> runningJob = null;
	static{
		runningJob = new ArrayBlockingQueue<MrJob>(Config.getIntegerProperty("runningJobQueue.size"), true);
	}
	
	public static void addRunningJob(MrJob mrJob){
		try {
			runningJob.put(mrJob);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	public static MrJob takeRunningJob(){
		try {
			return runningJob.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public synchronized static boolean removeJob(MrJob job){
		return runningJob.remove(job);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}

}
