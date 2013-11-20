/**
 * @工程 mr job center
 * @文件 ConfTemplateContainer.java
 * @时间 2013年9月6日 下午4:02:49
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.HashMap;

import org.sbs.platform.mrcenter.cobcontainer.dao.ConfTemplate;

import com.google.common.collect.Maps;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * 配置模板容器,该容器已被ConfTemplateContainer代替。
 */
@Deprecated
public class ConfTemplateContainer2 {
	private static HashMap<Integer, ConfTemplate> container = Maps.newHashMap();
	
	/**
	 * 从数据库中加载模版信息
	 */
	public void init(){
		// TODO 从数据中加载模板
	}
	
	/**
	 * 获取模板id对应的模板
	 * @param templateId
	 * @return
	 */
	public ConfTemplate getConfTemplate(Integer templateId){
		return container.get(templateId);
	}
	
	/**
	 * 添加一个模板，当数据库修改时，需要调用该方法以保证内存与数据库中的数据一致。
	 * @param templateId
	 * @param template
	 */
	public void addConfTemplate(Integer templateId,ConfTemplate template){
		container.put(templateId, template);
	}
	/**
	 * 修改一个模板，当数据库修改时，需要调用该方法以保证内存与数据库中的数据一致。
	 * @param templateId
	 * @param template
	 */
	public void updateTemplate(Integer templateId,ConfTemplate template){
		container.put(templateId,template);
	}
	/**
	 * 当删除掉一个模版时需要调用该方法，以保证数据一致。
	 * @param templateId
	 */
	public void remove(Integer templateId){
		container.remove(templateId);
	}
	/**
	 * destroy
	 */
	public void destroy(){
		container.clear();
		container = null;
	}
	/**
	 * 重新初始化容器
	 */
	public void reInit(){
		container.clear();
		init();
	}
}
