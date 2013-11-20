/**
 * @工程 mr job center
 * @文件 ConfTemplate.java
 * @时间 2013年9月6日 下午3:49:31
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.nutz.dao.entity.annotation.Column;
import org.nutz.dao.entity.annotation.Id;
import org.nutz.dao.entity.annotation.Table;

import com.google.common.collect.Maps;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * mr任务配置模板，对公用的参数配置以模版形式提供。
 */
@Table("TB_HADOOP_JOB_CONF_TEMPLATE")
public class ConfTemplate {
	@Column
	@Id
	private Integer id;
	/** 模版名称 **/
	@Column
	private String name;
	/** 模板内容 **/
	@Column
	private String content;
	
//	private HashMap<String, String> map = 
	
	public ConfTemplate() {
		super();
	}
	public ConfTemplate(Integer id, String name, String content) {
		super();
		this.id = id;
		this.name = name;
		this.content = content;
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
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		if(StringUtils.isNotBlank(content)){
			this.content = content.replace("\r", "");
		}
	}
	/**
	 * get content as map
	 * @return
	 */
	public HashMap<String, String> getContentMap(){
		HashMap<String, String> map = Maps.newHashMap();
		if(StringUtils.isNotBlank(this.content)){
			String sp = "\n";
			String strs[] = this.content.split(sp);
			for(String s:strs){
				if(!s.startsWith("#")){
					String ss[] = s.split("=");
					if(ss.length==2){
						map.put(ss[0], ss[1]);
					}
				}
			}
		}
		return map;
	}
	/**
	 * 读取文件。
	 * @return
	 */
	public Map<String, String> getConfMap(){
		XMLConfiguration config;
		try {
			config = new XMLConfiguration("E:\\sbsworkspace\\mr job center\\src\\main\\resources\\basic\\flow\\parameters_day_flow_visitleave1.xml");
			
			List fields = config.configurationsAt("properties.property");
			for (Iterator it = fields.iterator(); it.hasNext();) {
				HierarchicalConfiguration sub = (HierarchicalConfiguration) it.next();
				String fieldName = sub.getString("name");
				String value = sub.getString("value");
				System.out.println(fieldName+"="+value);
				
			}
		} catch (ConfigurationException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ConfTemplate [id=");
		builder.append(id);
		builder.append(", name=");
		builder.append(name);
		builder.append(", content=");
		builder.append(content);
		builder.append("]");
		return builder.toString();
	}
	
	
}
