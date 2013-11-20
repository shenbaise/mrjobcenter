/**
 * @工程 mrJobCenter
 * @文件 Start.java
 * @时间 2013年11月18日 下午7:31:50
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer;

import org.apache.commons.lang.StringUtils;
import org.sbs.platform.mrcenter.cobcontainer.classloader.MrClassLoader;
import org.sbs.platform.mrcenter.cobcontainer.schedule.MRSchedule;
import org.sbs.platform.mrcenter.cobcontainer.schedule.TestFlag;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc
 */
public class Start {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
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
