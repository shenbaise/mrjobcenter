/**
 * @工程 mr job center
 * @文件 CronTrigger.java
 * @时间 2013年9月10日 上午10:21:40
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.schedule;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.sbs.platform.mrcenter.cobcontainer.classloader.MrClassLoader;
import org.sbs.platform.mrcenter.cobcontainer.container.JobBeanContainer;
import org.sbs.platform.mrcenter.cobcontainer.container.ScheduleMrJobContainer;
import org.sbs.platform.mrcenter.cobcontainer.dao.DaoService;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobBean;
import org.sbs.platform.mrcenter.cobcontainer.utils.Config;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc 定时任务调度器，负责mr任务的调度。</br> cron表达式，需要定时执行的job使用cron表达式，以$开头。</br>
 *       仅需执行一次切需立刻执行的留空.</br> 需要定点执行一次的任务，填写执行时间yyyy.MM.dd HH:mm:ss
 */
public class MRSchedule {
	Log log = LogFactory.getLog(this.getClass());
	SchedulerFactory sf = new StdSchedulerFactory();
	/** 用户周期性任务调度 **/
	Scheduler sched;
	/** 用户指定时间的一次性任务调度 **/
	ScheduledExecutorService sched2;
	// 不被quartz调度的通过Excutor执行
	ExecutorService pool = Executors.newFixedThreadPool(20);
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	public MRSchedule() {
		try {
			sched = sf.getScheduler();
			sched2 = Executors.newScheduledThreadPool(20);
		} catch (SchedulerException e) {
			e.printStackTrace();
			log.error(e.getMessage());
		}
	}

	/**
	 * 从job容器中获取job。根据job情况设置是否定时执行。</br> 被依赖的job不进行调度，当依赖的job被调度时，会自动执行被依赖的job。
	 */
	private void start() {
		// 被依赖的jobbeand的ID
		List<Integer> ids = DaoService.getBeReliedJobBeans();
		Set<Entry<Integer, JobBean>> set = JobBeanContainer.getInstance()
				.getContainer().entrySet();
		log.info("JobBeanContainer.size="+set.size());
		for (final Entry<Integer, JobBean> entry : set) {
			// 需要被依赖的job不进行直接调度
			if (ids.contains(entry.getKey()))
				continue;
			// 加入quartz调度
			String cron = entry.getValue().getCron();
			if (StringUtils.isNotBlank(cron)) {
				// ********* 不是以$开头表示定点执行一次
				if (cron.charAt(0) != '$') {
					runOnceJobAt(entry.getValue());
				} else {
					// ********* 周期执行
					addJob(entry.getValue());
				}
			} else {
				// *********** 为空表示即刻执行一次
				runOnceJob(entry.getValue().getId());
			}
		}
	}

	/**
	 * 执行一次性任务
	 */
	public void runOnceJob(final int jobBeanId) {
		Runnable r = new Runnable() {
			public void run() {
				try {
//					ScheduleMrJobContainer.getInstance()
//					.getContent(jobBeanId)
//					.run(new String[] {});
					Excutor.runJob(JobBeanContainer.getInstance().getContent(jobBeanId));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		pool.submit(r);
	}

	/**
	 * 执行一次性定时任务
	 * 
	 * @param jobBeanId
	 */
	public void runOnceJobAt(final JobBean jobBean) {
		long delay = 10000L; // 1秒
		try {
			Date date = sdf.parse(jobBean.getCron());
			delay = System.currentTimeMillis() - date.getTime();
			log.info(jobBean.getName() + "(" + jobBean.getId() + ")将在" + delay
					/ 1000 + "秒后开始执行");
			if (delay >= 0) {
				sched2.schedule(new Runnable() {
					@Override
					public void run() {
						try {
//							ScheduleMrJobContainer.getInstance()
//									.getContent(jobBean.getId())
//									.run(new String[] {});
							Excutor.runJob(JobBeanContainer.getInstance().getContent(jobBean.getId()));
						} catch (Exception e) {
							e.printStackTrace();
							log.debug(e.getMessage());
						}
					}
				}, delay, TimeUnit.MILLISECONDS);
			}
		} catch (ParseException e1) {
			e1.printStackTrace();
			log.debug(e1.getMessage());
		}
	}

	/**
	 * 将任务加入quartz，启动quartz任务。
	 */
	public void init() {
		start();
		try {
			sched.start();
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
		// 初始化配置
		Config.init();
	}

	/**
	 * 增加一个定时任务，给任务传递一个jobBean，和一个Class
	 * 
	 * @param jobBean
	 */
	public void addJob(JobBean jobBean) {
		JobDetail jobDetail = newJob(Excutor.class).withIdentity(
				jobBean.getId() + "", "defualtGourp").build();
		JobDataMap map = jobDetail.getJobDataMap();
		map.put("jobClass",
				ScheduleMrJobContainer.getInstance()
						.getContent(jobBean.getId()));
		map.put("jobBean", jobBean);
		// 只需要这个就可以了。
		map.put("jobBeanId", jobBean.getId());
		Trigger trigger = newTrigger()
				.withIdentity(jobBean.getName(), "defaultGroup")
				.withSchedule(cronSchedule(jobBean.getCron().substring(1)))
				.build();
		try {
			log.info(jobBean.getName() + "(" + jobBean.getId() + ")加入调度");
			sched.scheduleJob(jobDetail, trigger);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 移除一个任务
	 * 
	 * @param jobBean
	 * @return
	 */
	public boolean removeJob(JobBean jobBean) {
		try {
			return sched.deleteJob(new JobKey(jobBean.getId() + "",
					"defualtGourp"));
		} catch (SchedulerException e) {
			e.printStackTrace();
			return false;
		}
	}

	public void shutdown() {
		try {
			sched.clear();
			sched.shutdown(true);
		} catch (SchedulerException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (args != null && args.length > 0 && "test".equals(args[0])) {
			TestFlag.test = true;
			if (args.length == 2 && StringUtils.isNotBlank(args[1])) {
				TestFlag.flag = args[1];
			}
		} else {
			TestFlag.test = false;
		}
//		TestFlag.test = true;
		// 初始化类动态加载器
		try {
			MrClassLoader.init();
		} catch (Exception e) {
			e.printStackTrace();
		}
		MRSchedule cron = new MRSchedule();
		cron.init();
		// cron.shutdown();
	}
}
