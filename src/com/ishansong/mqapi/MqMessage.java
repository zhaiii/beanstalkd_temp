package com.ishansong.mqapi;

/**
 * a mq message or job
 */
public class MqMessage {
	
	private byte[] data;
	private long id;
	private IMqClient cleint;

	public IMqClient getCleint() {
		return cleint;
	}
	public void setCleint(IMqClient cleint) {
		this.cleint = cleint;
	}
	public byte[] getData() {
		return this.data;
	}
	public void setData(byte[] data) {
		this.data = data;
	}
	public long getId() {
		return this.id;
	}
	public void setId(long id) {
		this.id = id;
	}

}
