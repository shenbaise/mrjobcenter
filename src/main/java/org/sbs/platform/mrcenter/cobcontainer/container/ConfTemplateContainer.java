/**
 * @工程 mr job center
 * @文件 ConfTemplaterContainer.java
 * @时间 2013年9月9日 上午11:02:03
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container;

import java.util.List;
import java.util.Map;

import org.sbs.platform.mrcenter.cobcontainer.dao.ConfTemplate;
import org.sbs.platform.mrcenter.cobcontainer.dao.DaoService;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc
 */
public class ConfTemplateContainer extends CommonContainer<Integer, ConfTemplate>{
	
	private static ConfTemplateContainer instance;
	
	private ConfTemplateContainer(){}
	
	public static ConfTemplateContainer getInstance(){
		if(instance==null){
			instance = new ConfTemplateContainer();
		}
		return instance;
	}

	@Override
	public boolean init() {
		List<ConfTemplate> list = DaoService.getConfTemplates();
		for(ConfTemplate template:list){
			super.addContent(template.getId(),template);
		}
		return true;
	}
	
	public static void main(String[] args) {
		Map<String, String> map = ConfTemplateContainer.getInstance().getContent(1).getContentMap();
		System.out.println(map);
	}

}
