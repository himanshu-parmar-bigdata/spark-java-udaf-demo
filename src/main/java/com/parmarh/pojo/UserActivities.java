package com.parmarh.pojo;

import java.io.Serializable;

public class UserActivities implements Serializable{
	
	String swid;
	String device_category;
	long time_spent;
	long video_start;
	
	
	public long getTime_spent() {
		return time_spent;
	}
	public void setTime_spent(long time_spent) {
		this.time_spent = time_spent;
	}
	public long getVideo_start() {
		return video_start;
	}
	public void setVideo_start(long video_start) {
		this.video_start = video_start;
	}
	public String getSwid() {
		return swid;
	}
	public void setSwid(String swid) {
		this.swid = swid;
	}
	public String getDevice_category() {
		return device_category;
	}
	public void setDevice_category(String device_category) {
		this.device_category = device_category;
	}


}
