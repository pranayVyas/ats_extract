package com.hortonworks.ats_extract;

/**
 * App_Detail.java - This contains the data model for application detail file
 * 
 * @author Pranay Ashok Vyas
 * @version 1.0
 * @see HP Inc.
 * @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
 *            licensed to you pursuant to the written agreement between
 *            Hortonworks and your company. If no such written agreement exists,
 *            you do not have a license to this software
 */
public class App_Detail {

	@Override
	public String toString() {
		return String.format(
				"App_Detail [appId=%s, attId=%s, userId=%s, amLaunchTs=%s, amStartTs=%s, attCount=%s, containerSize=%s]",
				appId, attId, userId, amLaunchTs, amStartTs, attCount, containerSize);
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getAttId() {
		return attId;
	}

	public void setAttId(String attId) {
		this.attId = attId;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getAmLaunchTs() {
		return amLaunchTs;
	}

	public void setAmLaunchTs(String amLaunchTs) {
		this.amLaunchTs = amLaunchTs;
	}

	public String getAmStartTs() {
		return amStartTs;
	}

	public void setAmStartTs(String amStartTs) {
		this.amStartTs = amStartTs;
	}

	public long getAttCount() {
		return attCount;
	}

	public void setAttCount(long attCount) {
		this.attCount = attCount;
	}

	private String appId;
	private String attId;
	private String userId;
	private String amLaunchTs;
	private String amStartTs;
	private long attCount;
	public long getContainerSize() {
		return containerSize;
	}

	public void setContainerSize(long containerSize) {
		this.containerSize = containerSize;
	}

	private long containerSize;
}
