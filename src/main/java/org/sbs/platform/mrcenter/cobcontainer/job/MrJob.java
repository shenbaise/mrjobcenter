/**
 * @工程 mr job center
 * @文件 MrJob.java
 * @时间 2013年9月6日 下午3:59:40
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.job;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.sbs.platform.mrcenter.cobcontainer.container.ConfTemplateContainer;
import org.sbs.platform.mrcenter.cobcontainer.container.JobBeanContainer;
import org.sbs.platform.mrcenter.cobcontainer.container.MrJobContainer;
import org.sbs.platform.mrcenter.cobcontainer.container.RunningJobQueue;
import org.sbs.platform.mrcenter.cobcontainer.container.ScheduleMrJobContainer;
import org.sbs.platform.mrcenter.cobcontainer.dao.ConfTemplate;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobBean;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobExcuteType;
import org.sbs.platform.mrcenter.cobcontainer.dao.JobStatus;
import org.sbs.platform.mrcenter.cobcontainer.exception.JobValidationException;
import org.sbs.platform.mrcenter.cobcontainer.schedule.Excutor;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc job 任务执行器。</br> 所有job，需要继承该类。</br> 请在beforrun方法中对job进行配置。
 */
public abstract class MrJob extends Configured implements Tool {

	private final Log log = LogFactory.getLog(this.getClass());
	protected Job job;
	protected JobBean jobBean;
	protected FileSystem fs;
	protected Configuration conf = new Configuration();
	protected Set<Path> inputPath = Sets.newHashSet();
	protected Set<Path> outputPath = Sets.newHashSet();
	/** 0表示hdfs、1表示db、2其他 **/
	protected int outputType = 0;

	public MrJob() {
	}

	/**
	 * job提交前，对job进行设置，inputPath、outputPath、mapper、reducer等。</br>
	 * 由于mr任务可能以jar的方式提交，在实现时请通过job.setJarByClass()设置jar
	 * 如果有串联子任务需要用本任务的输出作为输入，则返回输出路径。
	 * 
	 * @param args
	 * @return 返回输出路径
	 */
	public abstract List<Path> beforrun(String args[]);

	/**
	 * job运行之后（可记录执行结果）。1表示执行成功。0表示执行失败。3表示未执行
	 */
	public abstract void afterrun(int code);

	/**
	 * 通用配置
	 */
	public void configuration() {
		log.info("加载校验维度 ");
		if (StringUtils.isNotBlank(jobBean.getDimensions()))
			job.getConfiguration().set("dimensions", jobBean.getDimensions());
		log.info("解析配置文件中的指标 ");
		if (StringUtils.isNotBlank(jobBean.getIndicators()))
			job.getConfiguration().set("indicators", jobBean.getIndicators().replace("\n", ","));
		log.info("加载优先级项到环境 ");
		if (StringUtils.isNotBlank(jobBean.getPriority()))
			job.getConfiguration().set("mapred.job.priority", jobBean.getPriority());
		log.info("加载reduce数量项到环境 ");
		if (jobBean.getReduce_num() > 0)
			job.setNumReduceTasks(jobBean.getReduce_num());
		log.info("加载配置项到环境 ");
		if (StringUtils.isNotBlank(jobBean.getTmpjars()))
			this.job.getConfiguration().set("tmpjars", jobBean.getTmpjars().replaceAll(";", ","));
		log.info("加载机型品牌信息");
		if (StringUtils.isNotBlank(jobBean.getBrand_ruleJson()))
			this.job.getConfiguration().set("brand-rules", jobBean.getBrand_ruleJson());
		log.info("加载分辨率信息");
		if (StringUtils.isNotBlank(jobBean.getResolution_rules()))
			this.job.getConfiguration().set("resolution-rules", jobBean.getResolution_rulesJson());

		// TODO 其他
		this.job.getConfiguration().set("tmpjars", "/runtime/lib/lzoindex.jar");
		// 自定义参数、数据库连接配置、压缩等
		Set<Entry<String, String>> set = jobBean.getArgsMap().entrySet();
		for (Entry<String, String> entry : set) {
			if (StringUtils.isNotBlank(entry.getValue())) {
				if ("tmpjars".equals(entry.getKey())) {
					this.conf.set(entry.getKey(), entry.getValue().replaceAll(";", ",").trim());
				} else if ("io.compression.codecs".equals(entry.getKey().trim())) {
					this.conf.set(entry.getKey(), entry.getValue().replaceAll(";", ","));
				} else
					this.job.getConfiguration().set(entry.getKey(), entry.getValue().trim());
			}
		}
		log.info("setJar...");
		this.job.setJarByClass(MrJob.class);
		// 设置父任务的输出路径，作为子任务的输入路径
		JobBean jobBean2 = JobBeanContainer.getInstance().getContent(jobBean.getId());
		String tempOutputPahts = jobBean2.getTemp_output();
		if (StringUtils.isNotBlank(tempOutputPahts)) {
			this.job.getConfiguration().set("inputPath", tempOutputPahts.trim());
		}

		// mr优化参数设置
		ConfTemplate confTemplate = ConfTemplateContainer.getInstance().getContent(jobBean.getTemplate_id());
		if (confTemplate != null) {
			HashMap<String, String> m = confTemplate.getContentMap();
			if (m != null) {
				Set<Entry<String, String>> confSet = m.entrySet();
				for (Entry<String, String> entry : confSet) {
					this.job.getConfiguration().set(entry.getKey(), entry.getValue().trim());
				}
			}
		}

		try {
			this.fs = FileSystem.get(this.job.getConfiguration());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 为了兼容，设置一个对应与就平台的map对象，该对象中包含，日志类别、粒度、时间等
	 * 
	 * @return
	 */
	public HashMap<String, String> getArgsMap() {
		HashMap<String, String> map = Maps.newHashMap();

		if (StringUtils.isNotBlank(jobBean.getLog_types())) {
			map.put("logTypes", jobBean.getLog_types());
			job.getConfiguration().set("logTypes", jobBean.getLog_types());
		}

		String granularity = jobBean.getGranularityStr();
		if (StringUtils.isNotBlank(granularity)) {
			map.put("granularity", granularity);
			job.getConfiguration().set("granularity", granularity);
		}
		
		map.put("mapReduceIndex", String.valueOf(jobBean.getId()));
		map.put("jobName", jobBean.getName());
		map.put("priority", jobBean.getPriority());

		// 设置父任务的输出路径，作为子任务的输入路径
		JobBean[] jobBeans = getParentJob(jobBean);
		if (null != jobBeans) {
			StringBuilder sb = new StringBuilder();
			log.info("父任务个数：" + jobBeans.length);
			for (JobBean jb : jobBeans) {
				JobBean jobBean2 = JobBeanContainer.getInstance().getContent(jb.getId());
				String tempOutputPahts = jobBean2.getTemp_output();
				if (StringUtils.isNotBlank(tempOutputPahts)) {
					sb.append(tempOutputPahts).append(",");

				}
			}
			if (sb.length() > 1) {
				String t = sb.substring(0, sb.length() - 1);
				map.put("inputPath", t);
				log.info("父任务的输出路径为：" + t);
				this.job.getConfiguration().set("inputPath", t);
			}
		}
		return map;
	}

	/**
	 * job提交流程：</br> 1.开发人员实现beforrun.在该方法中对job进行相关设置。</br> 2.检测job设置</br>
	 * 3.检测是否有需要依赖的job，如果有则先执行依赖的job。</br> 4.将自身加入到正在运行的容器中。执行job（自身）</br>
	 * 5.job成功或失败后，将其移出正在运行队列。</br> 6.善后动作（记录job任务到job执行历史数据库等）。
	 */
	@Override
	public int run(String[] args) throws Exception {
		jobBean.setLastRunTime(new Date());
		jobBean.setJobStatus(JobStatus.pending);
		log.info(jobBean.getName() + "#pending");
		updateJobBean(this.jobBean);
		// 先检查是否有依赖的job,父任务执行失败则，子任务不再执行
		runParentJob(this.jobBean);
		
		// 通用配置
		configuration();
		// job设置
		List<Path> path = beforrun(args);
		if (null != path)
			outputPath.addAll(path);
		this.jobBean.setTemp_output(getoutPutPaths());
		// job检测
		if (validateJob()) {
			synchronized (this) {
				// 加入到正在运行的job容器中,队列满则等待。
				RunningJobQueue.addRunningJob(this);
				// 提交自身
				this.job.submit();
				jobBean.setJobStatus(JobStatus.running);
				log.info(jobBean.getName() + "#running");
				updateJobBean(this.jobBean);
			}

			int code = this.job.waitForCompletion(true) ? 1 : 0;
			// 从在正在运行的job容器中移除
			RunningJobQueue.removeJob(this);
			MrJobContainer.getInstance().remove(jobBean.getId());
			if (jobBean.jobExcuteType().equals(JobExcuteType.cycle)) {
				jobBean.setJobStatus(JobStatus.waiting);
			} else {
				jobBean.setJobStatus(JobStatus.finished);
			}
			// 设置job运行结果
			if (code == 1)
				jobBean.setSuccess(true);
			else
				jobBean.setSuccess(false);
			log.info(jobBean.getName() + "#" + jobBean.getJobStatus().name());
			updateJobBean(this.jobBean);
			// 善后
			afterrun(code);
			
			return code;
		} else {
			return 0;
		}
	}

	/**
	 * 更新JobBeanContainer中jobbean的状态
	 * 
	 * @param jobBean
	 */
	public synchronized void updateJobBean(JobBean jobBean) {
		JobBeanContainer.getInstance().addContent(jobBean.getId(), jobBean);
	}

	/**
	 * 获取父任务的输出路径
	 * 
	 * @param jobBean
	 * @return
	 */
	public List<Path> getOutPutPathOfParentJob(JobBean jobBean) {
		List<Path> paths = Lists.newArrayList();
		JobBean[] jobBeans = getParentJob(jobBean);
		if (null != jobBeans && jobBeans.length > 0) {
			for (JobBean jobBean2 : jobBeans) {
				paths.addAll(ScheduleMrJobContainer.getInstance().getContent(jobBean2.getId()).outputPath);
			}
		}
		log.info("父任务的输出路径有:" + paths.size());
		return paths;
	}

	/**
	 * 返回该任务的输出路径，该输出路径是beforerun方法的返回值
	 * 
	 * @return
	 */
	public String getoutPutPaths() {
		if (null != outputPath && outputPath.size() > 0) {
			StringBuilder sb = new StringBuilder();
			for (Path path : outputPath) {
				sb.append(path.toString()).append(",");
			}
			if (sb.length() > 1) {
				return sb.substring(0, sb.length() - 1);
			}
		}
		return "";
	}

	/**
	 * 从JobBeanContainer中获取依赖的父job
	 * 
	 * @return
	 */
	public JobBean[] getParentJob(JobBean jobBean) {
		if (StringUtils.isNotBlank(jobBean.getParent_job_ids())) {
			String[] ids = jobBean.getParent_job_ids().split(",");
			JobBean[] jobBeans = new JobBean[ids.length];
			int $id = -1;
			int i = 0;
			for (String id : ids) {
				$id = Integer.parseInt(id);
				jobBeans[i++] = JobBeanContainer.getInstance().getContent($id);
			}
			return jobBeans;
		}
		return null;
	}

	/***
	 * 获取子任务
	 * 
	 * @param jobBean
	 * @return
	 */
	public JobBean[] getChildJob(JobBean jobBean) {
		if (StringUtils.isNotBlank(jobBean.getParent_job_ids())) {
			String[] ids = jobBean.getParent_job_ids().split(",");
			JobBean[] jobBeans = new JobBean[ids.length];
			int $id = -1;
			int i = 0;
			for (String id : ids) {
				$id = Integer.parseInt(id);
				jobBeans[i++] = JobBeanContainer.getInstance().getContent($id);
			}
			return jobBeans;
		}
		return null;
	}

	/**
	 * 执行依赖的job(递归)
	 * 
	 * @param jobBean
	 * @return
	 */
	public boolean runParentJob(JobBean jobBean) {
		JobBean[] jobBeans = getParentJob(jobBean);
		if (null != jobBeans && jobBeans.length > 0) {
			int code = 0;
			ExecutorService pool = Executors.newFixedThreadPool(jobBeans.length);
			List<Future<Integer>> futures = Lists.newArrayList();
			for (final JobBean jobBean2 : jobBeans) {
				// 移步提交任务
				Future<Integer> future = pool.submit(new Callable<Integer>() {
					
					@Override
					public Integer call() throws Exception {
						JobBean jobBeanX = JobBeanContainer.getInstance().getContent(jobBean2.getId());
						if(null!=jobBeanX.getJobStatus())
							log.info("父任务status ="+jobBeanX.getJobStatus().name());
						else 
							log.info("父任务status =" + "未被设置");
						log.info("父任务lasttime =" +jobBeanX.getLastRunTime());
						// 父任务正在被调用，且未被完成
						if (jobBeanX.isInvoking()) {
							log.info("invoking ...");
							// 等待父任务执行完成
							while (jobBeanX.isInvoking()) {
								log.info("父任务：" + jobBeanX.getName() + "#已经被调用,等待一分钟");
								Thread.sleep(60000L);
								jobBeanX = JobBeanContainer.getInstance().getContent(jobBean2.getId());
							}
							// 执行完毕后或的执行结果（成、败）
							Thread.sleep(2000L);
							JobBean jobx = JobBeanContainer.getInstance().getContent(jobBeanX.getId());
							if (null != jobx) {
								return jobx.isSuccess() ? 1 : 0;
							}
							return 0;
						} else {
							log.info("not invoking");
							// 本次执行周期完成.(处于waitting状态，且当前时间与上次执行时间之差小于执行周期的0.98倍)
							if(jobBeanX.isFinishedThisRun()){
								log.info("is waitting");
								JobBean jobx = JobBeanContainer.getInstance().getContent(jobBeanX.getId());
								if (null != jobx) {
									return jobx.isSuccess() ? 1 : 0;
								}
								return 0;
							}else {	// 没有被执行，则执行之
								log.info("normal run");
								return Excutor.runJob(jobBeanX);
							}
						}
					}
				});
				if(null!=future)
					futures.add(future);
			}
			pool.shutdown();
			// 等待任务完成
			for (Future<Integer> future : futures) {
				try {
					// 在这里实现超时机制，超过2小时的任务不再执行
					code += future.get(2, TimeUnit.HOURS);
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				} catch (TimeoutException e) {
					code += 0;
					e.printStackTrace();
				}
			}
			// 依赖job全部完成
			if (code == jobBeans.length) {
				return true;
			} else {
				return false;
			}
		} else { // 无依赖job
			return true;
		}
	}

	/**
	 * 校验job
	 * 
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public boolean validateJob() throws JobValidationException {
		// 校验job对象
		if (null == this.job) {
			throw new JobValidationException("job对象为空！");
		}
		// 校验job名称
		if (StringUtils.isBlank(this.job.getJobName())) {
			try {
				throw new JobValidationException("jobName没有设置，系统将自动设置一个随机名称！");
			} catch (Exception e) {

			} finally {
				this.job.setJobName(UUID.randomUUID().toString().substring(0, 20));
			}
		}
		try {
			// 校验map类
			Class clazz = this.job.getMapperClass();
			if (null == clazz) {
				throw new JobValidationException("没有设置MapperClass！");
			}
			clazz = this.job.getInputFormatClass();
			if (null == clazz) {
				throw new JobValidationException("没有设置InputFormatClass！");
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			log.error(e.getMessage());
			return false;
		}
		return true;
	}

	public Job getJob() {
		return job;
	}

	public void setJob(Job job) {
		this.job = job;
	}

	public JobBean getJobBean() {
		return jobBean;
	}

	/** 通过classload创建实例后调用该方法 **/
	public void setJobBean(JobBean jobBean) {
		this.jobBean = jobBean;
		try {
			this.job = new Job(this.conf);
			this.conf = this.job.getConfiguration();
			this.job.setJobName(jobBean.getName());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public boolean isRunning() {
		return this.jobBean.isRunning();
	}

	public void setRunning(boolean running) {
		this.jobBean.setJobStatus(JobStatus.running);
	}
}
