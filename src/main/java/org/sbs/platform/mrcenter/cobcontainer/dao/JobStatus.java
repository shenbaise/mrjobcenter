/**
 * @工程 easoustat_0.0.4
 * @文件 JobStatus.java
 * @时间 2013年9月25日 下午4:22:04
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.dao;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc job状态
 */
public enum JobStatus {
	waiting/**等待被调度**/,
	pending/**排队等待提交**/,
	running/**正在执行**/,
	finished/**已结束，完成**/
}
