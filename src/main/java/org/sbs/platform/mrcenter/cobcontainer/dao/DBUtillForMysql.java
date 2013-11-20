/**
 * @工程 mr job center
 * @文件 DBUtillForMysql.java
 * @时间 2013年9月10日 下午4:32:51
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.dao;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nutz.dao.Condition;
import org.nutz.dao.Dao;
import org.nutz.dao.entity.Entity;
import org.nutz.dao.impl.NutDao;


/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc
 */
public class DBUtillForMysql {
	static Log log = LogFactory.getLog(DBUtillForMysql.class);
	public static Dao	dao	= null;
	
	public static Properties properties;

	static {
		if (properties == null) {
			properties = new Properties();
			try {
				properties.load(DBUtillForMysql.class.getResourceAsStream("/jdbc.properties"));
				log.info("数据库配置加载完成");
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		BasicDataSource ds = new BasicDataSource();
		ds.setDriverClassName(properties.getProperty("driver"));
		ds.setUsername(properties.getProperty("username"));
		ds.setPassword(properties.getProperty("password"));
		ds.setUrl(properties.getProperty("url"));
		
		dao = new NutDao(ds);
		log.info("数据库连接建立");
	}
	
	public static void main(String[] args) {
		int c = dao.count("tb_hadoop_job");
		
		Condition cnd = new Condition() {
			
			@Override
			public String toSql(Entity<?> entity) {
				return null;
			}
		};
		dao.query("tb_hadoop_job", cnd);
		System.out.println(c);
	}
}
