/**
 * @工程 mr job center
 * @文件 JobBean.java
 * @时间 2013年9月6日 下午3:32:33
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.dao;

import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.util.Tool;
import org.nutz.dao.entity.annotation.Column;
import org.nutz.dao.entity.annotation.Id;
import org.nutz.dao.entity.annotation.Table;
import org.nutz.json.Json;
import org.nutz.json.JsonFormat;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc job任务bean。一个实体对应一个mr job 注意事项：
 *       1.mr的分为定时执行、一次执行。需要定时执行的job使用cron表达式，以$开头。仅需执行一次的job填写0，任务mr上传后自动执行。
 *       需要定点执行一次的任务，填写执行时间YYYYmmDD，详细请看<b>cron</b>属性</br>
 *       2.输入、输出路径为基础路径，对于需要重复执行的任务，在程序中要在基础路径上构造新路径以防止结果被覆盖。</br>
 *       3.类名和jar包文件名名均不可重复。类名重复导致类冲突，jar文件名重复导致jar文件被覆盖。在新建job时需要在前端做校验。</br>
 *       4.一个jar里可能有多个mr任务，这些mr任务需要单独配置到数据库中。
 */

@Table("TB_HADOOP_JOB")
public class JobBean {
	@Column
	@Id
	private Integer id;
	/**
	 * job 名称
	 */
	@Column
	private String name;
	/**
	 * job详细描述
	 */
	@Column("describe")
	private String describe;
	/** 优先级 ，四个级别，1，2，3，4，5分别代表very high、high、normal、low，very low **/
	@Column("priority")
	private Integer priority;
	/**
	 * cron表达式，需要定时执行的job使用cron表达式，以$开头。</br> 仅需执行一次切需立刻执行的留空.</br>
	 * 需要定点执行一次的任务，填写执行时间YYYYmmDD HHMMss
	 **/
	@Column
	private String cron;
	
	/**
	 * mr状态。</br> 1-正在执行；</br> 2-等待执行中（对于需要定时重复执行的任务，要么等待执行，要么在执行）；</br>
	 * 3-执行完毕（对于仅执行一次的任务，要么等待执行，要么在执行，要么执行完毕）。
	 * private short status;
	 */
	
	/**
	 * 基本输入路径，路径中的动态部分（仅限路径末尾）需要在程序中构造。（例如以日期为文件名的路径）。</br>
	 * 1.多个绝对路径之间以，进行分隔</br>
	 * 2./xx/xx/${0001}#${8888}，将被解析为/xx/xx/0001,/xx/xx/0002。。。/xx/xx/8888。</br>
	 * #表示至1#9表示从1到9</br> 3./xx/xx/${date}-7#${date}，这种方式年、月会随天进行变化。<br>
	 * 4./xx/xx/${2013-09-01}#${2013-09-21} 5./xx/xx/${date} 6./xx/xx/${year}
	 */
	@Column
	private String input_path;
	/** 基本输出路径 **/
	@Column
	private String output_path;
	/** 类名 ，类的全路径。一个job对应一个类名不可重复 **/
	@Column("class_name")
	private String className;
	/**
	 * jar的上传路径，jar包文件名。不可重复，如果重复，plugin目录下其他mr的jar将被覆盖。jar命名规则：工程（部门）_任务功能_版本.
	 * jar
	 */
	@Column
	private String jar_path;
	/** 需要依赖的job，多个mr之间有相互依赖关系时适用.多个job之间以“,”分隔 **/
	@Column
	private String parent_job_ids;
	/** mapreduce任务参数优化模板，针对不同功能和性能需求的任务，配置不同参数模板，已到达对资源的合理利用 **/
	@Column
	private int template_id;
	/** 其他用户自定义参数 **/
	@Column
	private String args;
	/** 要执行的class **/
	private Class<Tool> mrClass;
	/** 支持的指标，以,号分隔。为了兼容之前的项目暂时保留 **/
	@Deprecated
	@Column
	private String indicators;
	/** 维度组合，以,号分隔。,为了兼容之前的项目暂时保留 **/
	@Deprecated
	@Column
	private String dimensions;

	@Column("TMPJARS")
	private String tmpjars;

	/** 兼容字段 **/
	@Deprecated
	@Column
	private int granularity;
	/** 兼容字段 **/
	@Deprecated
	@Column("RESOLUTION_RULES")
	private String resolution_rules;
	/** 兼容字段 **/
	@Deprecated
	@Column("BRAND_RULE")
	private String brand_rule;
	/** 兼容字段 **/
	@Column("LOG_TYPES")
	private String log_types;
	/** 用于存储每次临时输出的hdfs路径 **/
	@Column("TEMP_OUTPUT")
	private String temp_output;
	/** 默认的reduce数目 **/
	@Column
	private int reduce_num = 40;
	/** 上次执行时间，上次执行时间于当前时间之差小于执行周期，同时任务状态为2，表示本周起该任务已经执行完毕 **/
	private Date lastRunTime = null;
	/**执行结果**/
	private boolean success = true;
	/** job执行的状态，详见<b>JobStatus</b> **/
	private JobStatus jobStatus;

	public JobBean(Integer id, String name, String describe, Integer priority, String cron, String input_path,
			String output_path, String className, String jar_path, String parent_job_ids, Integer template_id,
			String args) {
		super();
		this.id = id;
		this.name = name;
		this.describe = describe;
		this.priority = priority;
		this.cron = cron;
		this.input_path = input_path;
		this.output_path = output_path;
		this.className = className;
		this.jar_path = jar_path;
		this.parent_job_ids = parent_job_ids;
		this.template_id = template_id;
		this.args = args;
	}

	public JobBean() {
		super();
	}

	/**
	 * 延迟加载class</br> 返回mrClass
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Class<Tool> getMrClass() {
		if (mrClass == null) {
			try {
				mrClass = (Class<Tool>) Class.forName(className);
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return mrClass;
	}

	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getDescribe() {
		return describe;
	}

	public void setDescribe(String describe) {
		this.describe = describe;
	}

	public String getPriority() {
		switch (priority) {
		case 1:
			return "VERY_HIGH";
		case 2:
			return "HIGH";
		case 3:
			return "NORMAL";
		case 4:
			return "LOW";
		case 5:
			return "VERY_LOW";

		default:
			return "NORMAL";
		}
	}

	public String getCron() {
		return cron;
	}

	public void setCron(String cron) {
		this.cron = cron;
	}

	public String getInput_path() {
		return input_path;
	}

	public void setInput_path(String input_path) {
		this.input_path = input_path;
	}

	public String getOutput_path() {
		return output_path;
	}

	public void setOutput_path(String output_path) {
		this.output_path = output_path;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getJar_path() {
		return jar_path;
	}

	public void setJar_path(String jar_path) {
		this.jar_path = jar_path;
	}

	public String getParent_job_ids() {
		return parent_job_ids;
	}

	public void setParent_job_ids(String parent_job_ids) {
		this.parent_job_ids = parent_job_ids;
	}

	public Integer getTemplate_id() {
		return template_id;
	}

	public void setTemplate_id(Integer template_id) {
		this.template_id = template_id;
	}

	public String getArgs() {
		return args;
	}

	public HashMap<String, String> getArgsMap() {
		HashMap<String, String> map = Maps.newHashMap();
		if (StringUtils.isNotBlank(this.args)) {
			this.args = this.args.replace("\r", "");
			String sp = "\n";
			String strs[] = this.args.split(sp);
			for (String s : strs) {
				if (!s.startsWith("#")) {
					String ss[] = s.split("=");
					map.put(ss[0], ss[1]);
				}
			}
		}
		return map;
	}

	public void setArgs(String args) {
		this.args = args;
	}


	public String getIndicators() {
		if (StringUtils.isNotBlank(indicators)) {
			if(this.indicators.contains("\r"))
				this.indicators = indicators.replace("\r", "");
		}
		return indicators;
	}

	public void setIndicators(String indicators) {
		if (StringUtils.isNotBlank(indicators)) {
			this.indicators = indicators.replace("\r", "");
		}
	}

	public String getDimensions() {
		if (StringUtils.isNotBlank(dimensions)) {
			if(this.dimensions.contains("\r"))
				this.dimensions = dimensions.replace("\r", "");
		}
		return dimensions;
	}

	public void setDimensions(String dimensions) {
		if (StringUtils.isNotBlank(dimensions)) {
			this.dimensions = dimensions.replace("\r", "");
		}
	}

	public void setPriority(Integer priority) {
		this.priority = priority;
	}

	public String getTmpjars() {
		return tmpjars;
	}

	public void setTmpjars(String tmpjars) {
		this.tmpjars = tmpjars;
	}

	public void setMrClass(Class<Tool> mrClass) {
		this.mrClass = mrClass;
	}

	public String getGranularityStr() {
		switch (this.granularity) {
		case 1:
			return "YEAR";
		case 2:
			return "HALF_YEAR";
		case 3:
			return "QUARTER";
		case 4:
			return "MONTH";
		case 5:
			return "NATURE_WEEK";
		case 6:
			return "WEEK";
		case 7:
			return "DAY";
		case 8:
			return "HOUR";
		case 9:
			return "TEN_MINUTES";
		case 10:
			return "SPIDER_DAY";

		default:
			break;
		}
		return "DAY";
	}
	/**
	 * 返回一个执行周期的时间长度（毫秒）
	 * @return
	 */
	public long getPeriod() {
		switch (this.granularity) {
		case 1:
			return 365*24*60*60*1000L;
		case 2:
			return 365*24*60*60*1000L/2L;
		case 3:
			return 365*24*60*60*1000L/4L;
		case 4:
			return 30*24*60*60*1000L/12L;
		case 5:
			return 7*24*60*60*1000L;
		case 6:
			return 7*24*60*60*1000L;
		case 7:
			return 24*60*60*1000L;
		case 8:
			return 60*60*1000L;
		case 9:
			return 10*60*1000L;
		case 10:
			return 24*60*60*1000L;
		}
		return 24*60*60*1000L;
	}

	public int getGranularity() {
		return this.granularity;
	}

	public void setGranularity(int granularity) {
		this.granularity = granularity;
	}

	public String getResolution_rules() {
		return resolution_rules;
	}
	
	/**
	 * 获取分辨率解析规则（map）
	 * @return
	 */
	public String getResolution_rulesJson(){
		List<HashMap<String, String>> list = Lists.newArrayList();
		if(StringUtils.isNotBlank(this.resolution_rules)){
			this.resolution_rules = this.resolution_rules.replace("\r", "");
			String sp = "\n";
			String strs[] = this.resolution_rules.split(sp);
			for (String s : strs) {
				if (!s.startsWith("#")) {
					HashMap<String, String> map = Maps.newHashMap();
					String ss[] = s.split("=");
					map.put("name", ss[0]);
					map.put("value", ss[1]);
					map.put("ognl", "false");
					list.add(map);
				}
			}
		}
		String json = null;
		if(list.size()>0){
			json = Json.toJson(list, new JsonFormat(true).setIgnoreNull(true).setQuoteName(false));
		}
		return json;
	}

	public void setResolution_rules(String resolution_rules) {
		this.resolution_rules = resolution_rules;
	}

	public String getBrand_rule() {
		return brand_rule;
	}
	
	
	/**
	 * 获取机型解析规则
	 * @return
	 */
	public String getBrand_ruleJson(){
		List<HashMap<String, String>> list = Lists.newArrayList();
		if(StringUtils.isNotBlank(this.brand_rule)){
			this.brand_rule = this.brand_rule.replace("\r", "");
			String sp = "\n";
			String strs[] = this.brand_rule.split(sp);
			for (String s : strs) {
				if (!s.startsWith("#")) {
					HashMap<String, String> map = Maps.newHashMap();
					String ss[] = s.split("=");
					map.put("name", ss[0]);
					map.put("value", ss[1]);
					map.put("ognl", "false");
					list.add(map);
				}
			}
		}
		String json = null;
		if(list.size()>0){
			json = Json.toJson(list, new JsonFormat(true).setIgnoreNull(true).setQuoteName(false));
		}
		return json;
	}

	public void setBrand_rule(String brand_rule) {
		this.brand_rule = brand_rule;
	}

	public String getLog_types() {
		return log_types;
	}

	public void setLog_types(String log_types) {
		this.log_types = log_types;
	}

	public String getTemp_output() {
		return temp_output;
	}

	public void setTemp_output(String temp_output) {
		this.temp_output = temp_output;
	}

	public int getReduce_num() {
		return reduce_num;
	}

	public void setReduce_num(int reduce_num) {
		this.reduce_num = reduce_num;
	}
	
	public synchronized Date getLastRunTime() {
		return lastRunTime;
	}

	public synchronized void setLastRunTime(Date lastRunTime) {
		this.lastRunTime = lastRunTime;
	}

	public void setTemplate_id(int template_id) {
		this.template_id = template_id;
	}
	
	/**
	 * 返回任务的执行周期
	 * @return
	 */
	public JobExcuteType jobExcuteType(){
		if(StringUtils.isNotBlank(this.cron)){
			if(this.cron.startsWith("$"))
				return JobExcuteType.cycle;
		}
		return JobExcuteType.once;
	}
	
	public synchronized JobStatus getJobStatus() {
		return jobStatus;
	}

	public synchronized void setJobStatus(JobStatus jobStatus) {
		this.jobStatus = jobStatus;
	}
	/**
	 * 任务是否在运行中
	 * @return
	 */
	public synchronized boolean isRunning(){
		if(JobStatus.running.equals(this.jobStatus)){
			return true;
		}
		return false;
	}
	/**
	 * 是否在等待执行
	 * @return
	 */
	public synchronized boolean isPending(){
		if(JobStatus.pending.equals(this.jobStatus)){
			return true;
		}
		return false;
	}
	/**
	 * 是否等待调度中
	 * @return
	 */
	public synchronized boolean isWaitting(){
		if(JobStatus.waiting.equals(this.jobStatus)){
			return true;
		}
		return false;
	}
	/**
	 * 是否已经结束
	 * @return
	 */
	public synchronized boolean isFinished(){
		if(JobStatus.finished.equals(this.jobStatus)){
			return true;
		}
		return false;
	}
	/**
	 * 是否在当前执行周期内进行调度中
	 * @return
	 */
	public synchronized boolean isInvoking(){
		if(isRunning() || isPending()){
			return true;
		}
		return false;
	}
	
	/**
	 * 本次执行周期完成.(处于waitting状态，且当前时间与上次执行时间之差小于执行周期)
	 * @return
	 */
	public synchronized boolean isFinishedThisRun(){
		
		if(isWaitting()){
			if(lastRunTime!=null){
				long t = System.currentTimeMillis()-lastRunTime.getTime();
				// 本周期内调度完成，周期内不再执行。如果是下一轮调度，则t应该>=周期
				if(t<=getPeriod() * 0.98){
					return true;
				}
				// 本周期内未完成
				return false;
			}
			return false;
		}
		return false;
	}
	
	public synchronized boolean isSuccess() {
		return success;
	}

	public synchronized void setSuccess(boolean success) {
		this.success = success;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("JobBean [id=");
		builder.append(id);
		builder.append(", name=");
		builder.append(name);
		builder.append(", describe=");
		builder.append(describe);
		builder.append(", priority=");
		builder.append(priority);
		builder.append(", cron=");
		builder.append(cron);
		builder.append(", input_path=");
		builder.append(input_path);
		builder.append(", output_path=");
		builder.append(output_path);
		builder.append(", className=");
		builder.append(className);
		builder.append(", jar_path=");
		builder.append(jar_path);
		builder.append(", parent_job_ids=");
		builder.append(parent_job_ids);
		builder.append(", template_id=");
		builder.append(template_id);
		builder.append(", args=");
		builder.append(args);
		builder.append(", mrClass=");
		builder.append(mrClass);
		builder.append(", indicators=");
		builder.append(indicators);
		builder.append(", dimensions=");
		builder.append(dimensions);
		builder.append(", tmpjars=");
		builder.append(tmpjars);
		builder.append(", granularity=");
		builder.append(granularity);
		builder.append(", resolution_rules=");
		builder.append(resolution_rules);
		builder.append(", brand_rule=");
		builder.append(brand_rule);
		builder.append(", log_types=");
		builder.append(log_types);
		builder.append(", temp_output=");
		builder.append(temp_output);
		builder.append(", reduce_num=");
		builder.append(reduce_num);
		builder.append(", lastRunTime=");
		builder.append(lastRunTime);
		builder.append(", success=");
		builder.append(success);
		builder.append(", jobStatus=");
		builder.append(jobStatus);
		builder.append(", getMrClass()=");
		builder.append(getMrClass());
		builder.append(", getId()=");
		builder.append(getId());
		builder.append(", getName()=");
		builder.append(getName());
		builder.append(", getDescribe()=");
		builder.append(getDescribe());
		builder.append(", getPriority()=");
		builder.append(getPriority());
		builder.append(", getCron()=");
		builder.append(getCron());
		builder.append(", getInput_path()=");
		builder.append(getInput_path());
		builder.append(", getOutput_path()=");
		builder.append(getOutput_path());
		builder.append(", getClassName()=");
		builder.append(getClassName());
		builder.append(", getJar_path()=");
		builder.append(getJar_path());
		builder.append(", getParent_job_ids()=");
		builder.append(getParent_job_ids());
		builder.append(", getTemplate_id()=");
		builder.append(getTemplate_id());
		builder.append(", getArgs()=");
		builder.append(getArgs());
		builder.append(", getArgsMap()=");
		builder.append(getArgsMap());
		builder.append(", getIndicators()=");
		builder.append(getIndicators());
		builder.append(", getDimensions()=");
		builder.append(getDimensions());
		builder.append(", getTmpjars()=");
		builder.append(getTmpjars());
		builder.append(", getGranularityStr()=");
		builder.append(getGranularityStr());
		builder.append(", getPeriod()=");
		builder.append(getPeriod());
		builder.append(", getGranularity()=");
		builder.append(getGranularity());
		builder.append(", getResolution_rules()=");
		builder.append(getResolution_rules());
		builder.append(", getBrand_rule()=");
		builder.append(getBrand_rule());
		builder.append(", getLog_types()=");
		builder.append(getLog_types());
		builder.append(", getTemp_output()=");
		builder.append(getTemp_output());
		builder.append(", getReduce_num()=");
		builder.append(getReduce_num());
		builder.append(", getLastRunTime()=");
		builder.append(getLastRunTime());
		builder.append(", jobExcuteType()=");
		builder.append(jobExcuteType());
		builder.append(", getJobStatus()=");
		builder.append(getJobStatus());
		builder.append(", isRunning()=");
		builder.append(isRunning());
		builder.append(", isPending()=");
		builder.append(isPending());
		builder.append(", isWaitting()=");
		builder.append(isWaitting());
		builder.append(", isFinished()=");
		builder.append(isFinished());
		builder.append(", isFinishedThisRun()=");
		builder.append(isFinishedThisRun());
		builder.append(", isSuccess()=");
		builder.append(isSuccess());
		builder.append(", getClass()=");
		builder.append(getClass());
		builder.append(", hashCode()=");
		builder.append(hashCode());
		builder.append(", toString()=");
		builder.append(super.toString());
		builder.append("]");
		return builder.toString();
	}
}
