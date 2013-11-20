/**
 * @工程 mr job center
 * @文件 Excutor.java
 * @时间 2013年9月10日 上午11:51:33
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.schedule;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.sbs.platform.mrcenter.cobcontainer.classloader.MrClassLoader;
import org.sbs.platform.mrcenter.cobcontainer.container.JobBeanContainer;
import org.sbs.platform.mrcenter.cobcontainer.container.MrJobContainer;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobBean;
import org.sbs.platform.mrcenter.cobcontainer.job.MrJob;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc
 */
public class Excutor implements Job{
	static Log log = LogFactory.getLog(Excutor.class);
	@Override
	public void execute(JobExecutionContext context)
			throws JobExecutionException {
		// 拿到MrJob
//		MrJob job = (MrJob) context.getJobDetail().getJobDataMap().get("jobClass");
		JobBean jobBean = (JobBean) context.getJobDetail().getJobDataMap().get("jobBean");
		Object z;
		try {
			jobBean = JobBeanContainer.getInstance().getContent(jobBean.getId());
			z = MrClassLoader.loadClass(jobBean.getClassName()).newInstance();
			if(null!=z && z instanceof MrJob){
				MrJob mrJob = (MrJob)z;
				mrJob.setJobBean(jobBean);
				try {
					mrJob.run(new String[]{});
				} catch (Exception e) {
					log.error(e.getMessage());
					e.printStackTrace();
				}
			}else {
				log.error("Class加载失败#"+jobBean.getClassName());
			}
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		}
	}
	
	/**
	 * 执行job
	 * @param jobBean
	 * @return
	 */
	public static int runJob(JobBean jobBean){
		Object z;
		try {
			MrJob mrJob = MrJobContainer.getInstance().getContent(jobBean.getId());
			if(null==mrJob){
				z = MrClassLoader.loadClass(jobBean.getClassName()).newInstance();
				if(null!=z && z instanceof MrJob){
					mrJob = (MrJob)z;
					mrJob.setJobBean(jobBean);
					try {
						MrJobContainer.getInstance().addContent(jobBean.getId(), mrJob);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}else {
					log.error("Class加载失败#"+jobBean.getClassName());
				}
			}
			return mrJob.run(new String[]{});
		} catch (InstantiationException e1) {
			e1.printStackTrace();
		} catch (IllegalAccessException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
}
