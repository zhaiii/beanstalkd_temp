/**
 * 
 */
package com.ishansong.mqapi;

/**
 * Mq operation Exception
 *
 */
public class MqException extends Exception {
	private static final long serialVersionUID = 542716106054399999L;

	public MqException() {
		this(null, null);
	}

	public MqException(String message) {
		this(message, null);
	}

	public MqException(String message, Exception cause) {
		super(message, cause);
	}

	public MqException(Exception cause) {
		this(null, cause);
	}
}
