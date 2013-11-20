/**
 * @工程 mr job center
 * @文件 DaoService.java
 * @时间 2013年9月11日 上午10:15:38
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.dao;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.nutz.dao.Cnd;
import org.nutz.dao.Dao;
import org.sbs.platform.mrcenter.cobcontainer.schedule.TestFlag;

import com.google.common.collect.Lists;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc 数据库服务类。主要用于对job、config_template的相关操作
 */
public class DaoService {
	
	public static Dao dao = DBUtillForMysql.dao;
	
	public static void main(String[] args) {
		TestFlag.flag = "1";
		TestFlag.test = true;
		List<JobBean> list = getAllJobBeans();
		JobBean bean = list.get(2);
		bean.setJobStatus(JobStatus.pending);
		for(JobBean b:list){
			
			/*String str = b.getIndicators();
				if(StringUtils.isNotBlank(str)){
				str = str.replace("\n",",");
				List<String> arr = CommonUtils.split(str, ",");
				List indicators = new ArrayList();
				for (String s : arr) {
					if(s.startsWith("#"))
						continue;
					if (StringUtils.isNotBlank(s)) {
						indicators.add(SummaryIndicators.valueOf(s));
					}
				}
			}*/
			
			String str = b.getDimensions();
			
			if(StringUtils.isNotBlank(str)){
				if(str.contains("\r")){
					System.out.println("##3");
				}
			}
				
		}
			
		System.out.println(list.size());
		System.out.println(getAllJobBeans4test().size());
		
		System.out.println(getBeReliedJobBeans().size());
		System.out.println(getBeReliedJobBeans4test().size());
	}
	/**
	 * 获取到全部的(正式)job信息
	 * @return
	 */
	public static List<JobBean> getAllJobBeans(){
		if(TestFlag.test){
			return getAllJobBeans4test();
		}
		return dao.query(JobBean.class, Cnd.wrap("test_flag = 0"));
	}
	/**
	 * 获取到全部用于测试的job
	 * @return
	 */
	public static List<JobBean> getAllJobBeans4test(){
		return dao.query(JobBean.class, Cnd.wrap("test_flag = "+TestFlag.flag));
	}
	/**
	 * 获取到需要被依赖的job(正式)的ID
	 * @return
	 */
	public static List<Integer> getBeReliedJobBeans(){
		if(TestFlag.test){
			return getBeReliedJobBeans4test();
		}
		List<JobBean> beReliedJobBeans = dao.query(JobBean.class, Cnd.wrap("parent_job_ids is not null and test_flag = 0"));
		List<Integer> beReliedJobBeanIds = Lists.newArrayList();
		for(JobBean jobBean:beReliedJobBeans){
			String pids = jobBean.getParent_job_ids();
			if(StringUtils.isNotBlank(pids)){
				String[] ss = pids.split(",");
				for(String s :ss){
					beReliedJobBeanIds.add(Integer.parseInt(s));
				}
			}
		}
		return beReliedJobBeanIds;
	}
	
	/**
	 * 获取到需要被依赖的job(正式)的ID
	 * @return
	 */
	public static List<Integer> getBeReliedJobBeans4test(){
		List<JobBean> beReliedJobBeans = dao.query(JobBean.class, Cnd.wrap("parent_job_ids is not null and test_flag = "+TestFlag.flag));
		List<Integer> beReliedJobBeanIds = Lists.newArrayList();
		for(JobBean jobBean:beReliedJobBeans){
			String pids = jobBean.getParent_job_ids();
			if(StringUtils.isNotBlank(pids)){
				String[] ss = pids.split(",");
				for(String s :ss){
					beReliedJobBeanIds.add(Integer.parseInt(s));
				}
			}
		}
		return beReliedJobBeanIds;
	}
	/**
	 * 返回所有模版
	 * @return
	 */
	public static List<ConfTemplate> getConfTemplates(){
		return dao.query(ConfTemplate.class, Cnd.wrap(""));
	}
	
	/**
	 * 返回子任务
	 * @return
	 */
	public static List<Integer> getChildJobBeans(){
		List<JobBean> childJobBeans = dao.query(JobBean.class, Cnd.wrap("parent_job_ids is not null"));
		List<Integer> beReliedJobBeanIds = Lists.newArrayList();
		for(JobBean jobBean:childJobBeans){
			String pids = jobBean.getParent_job_ids();
			if(StringUtils.isNotBlank(pids)){
				String[] ss = pids.split(",");
				for(String s :ss){
					beReliedJobBeanIds.add(Integer.parseInt(s));
				}
			}
		}
		return beReliedJobBeanIds;
	}
	/**
	 * 更新jobbean
	 * @param jobBean
	 * @return
	 */
	public boolean updateJobbean(JobBean jobBean){
		dao.update(jobBean);
		return true;
	}
}
