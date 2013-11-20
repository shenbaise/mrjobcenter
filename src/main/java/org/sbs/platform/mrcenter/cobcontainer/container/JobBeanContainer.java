/**
 * @工程 mr job center
 * @文件 JobContainer.java
 * @时间 2013年9月6日 下午3:53:03
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.List;

import org.sbs.platform.mrcenter.cobcontainer.dao.DaoService;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobBean;

/**
 * @author shenbaise（shenbaise1001@126.com） 任务容器，包含所有job运行需要的相关属性
 */
public class JobBeanContainer extends CommonContainer<Integer, JobBean> {

	private static JobBeanContainer instance;

	public synchronized static JobBeanContainer getInstance() {
		if (instance == null) {
			instance = new JobBeanContainer();
		}
		return instance;
	}
	
	/**
	 * 初始化，从数据库中加载jobBean。
	 */
	@Override
	public boolean init() {
		List<JobBean> jobBeans = DaoService.getAllJobBeans();
		for(JobBean jobBean :jobBeans){
			log.info("job【"+jobBean.getName()+"】加入容器");
			super.addContent(jobBean.getId(), jobBean);
		}
		return true;
	}

	public static void main(String[] args) {
		JobBeanContainer container = JobBeanContainer.getInstance();
		container.destroy();
		System.out.println("hello");
	}
}
