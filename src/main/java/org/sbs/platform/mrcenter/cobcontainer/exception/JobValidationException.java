/**
 * @工程 mr job center
 * @文件 JobValidationException.java
 * @时间 2013年9月9日 上午10:21:39
 * @作者  shenbaise（shenbaise1001@126.com）
 * @描述 
 */
package org.sbs.platform.mrcenter.cobcontainer.exception;

/**
 * @author shenbaise（shenbaise1001@126.com）
 * @desc job校验异常
 */
public class JobValidationException extends Exception {

	private static final long serialVersionUID = 1L;

	public JobValidationException() {
		super();
	}

	public JobValidationException(String message, Throwable cause) {
		super(message, cause);
	}

	public JobValidationException(String message) {
		super(message);
	}

	public JobValidationException(Throwable cause) {
		super(cause);
	}
	
}
