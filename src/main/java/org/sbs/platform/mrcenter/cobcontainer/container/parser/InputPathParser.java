/**
 * @工程 mr job center
 * @文件 InputPathParser.java
 * @时间 2013年9月11日 下午6:27:38
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.container.parser;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

import com.google.common.base.CharMatcher;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc
 */
public class InputPathParser {
	
	public static HashMap<String, String> expressionMap = Maps.newHashMap();
	public static final String date = "date";
	public static final String date_ = "${date}";
	public static final Pattern patternDateF = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy/MM/dd");
	
	/**
	 * 解析输入路径的动态部分，仅路径最后一部分可使用动态路径。
	 * @param input_path
	 * @return
	 */
	public static Path[] getPaths(String input_path){
		List<Path> paths = Lists.newArrayList();
		if(input_path.contains(",")){
			String[] ss = input_path.split(",");
			for(String s:ss){
				paths.add(new Path(s));
			}
			return paths.toArray(new Path[]{});
		}
		String basePath = "",dynamicPath = "";
		basePath = input_path.substring(0,input_path.lastIndexOf("/"));
		dynamicPath = input_path.substring(input_path.lastIndexOf("/")+1);
		SubPath subPath = new SubPath(dynamicPath);
		List<String> dynamicSubPaths =subPath.getSubPaths();
		StringBuilder sb = new StringBuilder();
		for(String s : dynamicSubPaths){
			sb.append(basePath).append("/").append(s);
			paths.add(new Path(sb.toString()));
			sb.delete(0, sb.length());
		}
		
		// 解析到具体文件？
		return paths.toArray(new Path[]{});
	}
	
	/**
	 * 生成动态部分。
	 * @param str
	 * @return
	 */
	public static List<String> dynamicString(String str){
		List<String> lists = Lists.newArrayList();
		
		return lists;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// 测试用例
		/*
		 * 1./xx/xx/a,/xx/xx/b,/xx/xx/y,/xx/xx/z 
		 * 2./xx/xx/${0001}#${8888}
		 * 3./xx/xx/${date}-7#${date}，这种方式年、月会随天进行变化。
		 * 4./xx/xx/${2013-09-01}#${2013-09-21}
		 * 5./xx/xx/${date}
		 * 6./xx/xx/${year}
		 */
		DecimalFormat format = new DecimalFormat(CharMatcher.DIGIT.replaceFrom("x0001", "0"));
		System.out.println(format.format(2));
		String case1 = "/xx/xx/${0001}#${0008}";
		String case2 = "/xx/xx/${xa0001}#${xa0018}";
		String case3 = "/xx/xx/${date}-7#${date}+2";
		String case4 = "/xx/xx/${2013-09-01}#${2013-09-21}";
		String case5 = "/xx/xx/${date}-1";
		String case6 = "/xx/xx/${year}";
		String case7 = "/xx/xx/2013/09/${day}";
		String case8 = "/xx/xx/a,/xx/xx/b,/xx/xx/y,/xx/xx/z";
		String case9 = "/xx/xx/abx";
		
		System.out.println(InputPathParser.getPaths(case1));
		System.out.println(InputPathParser.getPaths(case2));
		System.out.println(InputPathParser.getPaths(case3));
		System.out.println(InputPathParser.getPaths(case4));
		System.out.println(InputPathParser.getPaths(case5));
		System.out.println(InputPathParser.getPaths(case6));
		System.out.println(InputPathParser.getPaths(case7));
		System.out.println(InputPathParser.getPaths(case8));
		System.out.println(InputPathParser.getPaths(case9));
	}
	
	/**
	 * 动态子路径
	 * @author shenbaise（shenbaise1001@126.com）
	 * @desc
	 */
	public static class SubPath {
		/**
		 * 子路径（路径中的一个文件夹名）
		 */
		public String path;
		/**
		 * 该子路径是否为表达式
		 */
		public boolean exp;
		
		public SubPath(String path) {
			super();
			this.path = path;
		}

		/**
		 * 处理时间 ${day}-7#${day}
		 * @param datePath
		 * @return
		 */
		private Date parseDate(String datePath){
			Calendar calendar = Calendar.getInstance();
			if(date_.equals(datePath)){
				return calendar.getTime();
			}else {
				if(datePath.contains("-")){
					int i = Integer.parseInt(datePath.substring(datePath.indexOf('-')+1));
					calendar.add(Calendar.DAY_OF_MONTH, 0-i);
				}else if (datePath.contains("+")) {
					int i = Integer.parseInt(datePath.substring(datePath.indexOf('+')+1));
					calendar.add(Calendar.DAY_OF_MONTH, i);
				}
			}
			return calendar.getTime();
		}
		
		/**
		 * 处理时间 ${2013-09-01}#${2013-09-21}
		 * @param datePath
		 * @return
		 */
		private Date parseDate2(String datePath){
			if(patternDateF.matcher(datePath).matches()){
				try {
					return dateFormat.parse(datePath);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			return Calendar.getInstance().getTime();
		}
		
		
		/**
		 * 如果该子路径包含表达式。则根据表达式生成子路径。
		 * @return
		 */
		public List<String> getSubPaths(){
			List<String> paths = Lists.newArrayList();
			// ${0001}#${8888} | ${date}-7#${date} | ${2013-09-01}#${2013-09-21}
			if(path.contains("#")){
				String ss[] = path.split("#");
				String s1 = CharMatcher.anyOf("${}").removeFrom(ss[0]);
				String s2 = CharMatcher.anyOf("${}").removeFrom(ss[1]);
				// 第二三种情况
				Date startDate = null ,endDate = null;
				// start
				if(ss[0].startsWith(date_)){
					startDate = parseDate(ss[0]);
				}else if(patternDateF.matcher((s1)).matches()){
					startDate = parseDate2(s1);
				}
				// end
				if(ss[1].startsWith(date_)){
					endDate = parseDate(ss[1]);
				}else if(patternDateF.matcher((s2)).matches()){
					endDate = parseDate2(s2);
				}
				
				if(null!=startDate && null!=endDate){
					Calendar cal = Calendar.getInstance();
					cal.setTime(endDate);
					while(startDate.getTime()<=cal.getTime().getTime()){
						paths.add(dateFormat2.format(cal.getTime()));
						cal.add(Calendar.DAY_OF_MONTH, -1);
					}
					return paths;
				}
				
				
				String temp = "";
				DecimalFormat format = new DecimalFormat(CharMatcher.DIGIT.replaceFrom(s1,"0"));
				for(int i=s1.length()-1;i>=0;i--){
					if(s1.charAt(i) <= '9'
							&& s1.charAt(i) >= '0'){
						temp = s1.charAt(i) + temp;
					}else {
						break;
					}
				}
				int start = Integer.parseInt(temp);
				temp = "";
				for(int i=s2.length()-1;i>=0;i--){
					if(s2.charAt(i) <= '9' && s2.charAt(i) >= '0'){
						temp = s2.charAt(i) + temp;
					}else {
						break;
					}
				}
				int end = Integer.parseInt(temp);
				int i=start;
				for(;i<=end;i++){
					paths.add(format.format(i));
				}
				return paths;
			}if("${year}".equals(path)){
				paths.add(String.valueOf(Calendar.getInstance().get(Calendar.YEAR)));
			}else if("${mon}".equals(path)){
				paths.add(String.valueOf(Calendar.getInstance().get(Calendar.MONTH)));
			}else if("${day}".equals(path)){
				paths.add(String.valueOf(Calendar.getInstance().get(Calendar.DAY_OF_MONTH)));
			}else if("${date}".equals(path)){
				paths.add(dateFormat2.format(Calendar.getInstance().getTime()));
			}else {
				paths.add(path);
			}
			return paths;
		}
		
		@SuppressWarnings("unused")
		private String getNumberPart(String path){
			String temp = "";
			for(int i=path.length()-1;i>=0;i++){
				if(path.charAt(i) <= '9' && path.charAt(i) >= '0'){
					temp = path.charAt(i) + temp;
				}else {
					break;
				}
			}
			return temp;
		}
	}
}

