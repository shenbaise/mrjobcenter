/**
 * @工程 mr job center
 * @文件 RunningJobContainer.java
 * @时间 2013年9月9日 下午3:02:11
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.Map.Entry;
import java.util.Set;

import org.sbs.platform.mrcenter.cobcontainer.classloader.MrClassLoader;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobBean;
import org.sbs.platform.mrcenter.cobcontainer.job.MrJob;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc MR对象容器
 */
public class ScheduleMrJobContainer extends CommonContainer<Integer, MrJob> {
	
	private static ScheduleMrJobContainer instance ;
	
	public static ScheduleMrJobContainer getInstance(){
		if(null==instance){
			instance = new ScheduleMrJobContainer();
		}
		return instance;
	}
	
	/**
	 * 执行初始化,从jobbean容器中获取jobbean。根据jobbean创建MrJob实例。</br>
	 */
	@Override
	public boolean init() {
		JobBeanContainer jobBeanContainer = JobBeanContainer.getInstance();
		Set<Entry<Integer, JobBean>> set = jobBeanContainer.getContainer().entrySet();
		for(Entry<Integer, JobBean> entry:set){
			JobBean bean = entry.getValue();
			try {
//				Object z = Class.forName(bean.getClassName()).newInstance();
				Object z = MrClassLoader.loadClass(bean.getClassName()).newInstance();
				if(null!=z && z instanceof MrJob){
					MrJob mrJob = (MrJob)z;
					mrJob.setJobBean(bean);
					super.addContent(entry.getKey(), mrJob);
//					log.info("Class加载成功#"+bean.getClassName());
				}else {
					log.error("Class加载失败#"+bean.getClassName());
					return false;
				}
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		return true;
	}

}
