package com.hortonworks.ats_extract;

/**
 * Dag_Detail.java - This contains the data model for dag detail file
 * 
 * @author Pranay Ashok Vyas
 * @version 1.0
 * @see HP Inc.
 * @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
 *            licensed to you pursuant to the written agreement between
 *            Hortonworks and your company. If no such written agreement exists,
 *            you do not have a license to this software
 */

public class Dag_Detail {

	@Override
	public String toString() {
		return String.format(
				"Dag_Detail [appId=%s, dagId=%s, dagName=%s, callerId=%s, callerType=%s, description=%s, dagSubmitTs=%s, dagInitTs=%s, dagStartTs=%s, dagFinishTs=%s, queueName=%s]",
				appId, dagId, dagName, callerId, callerType, description, dagSubmitTs, dagInitTs, dagStartTs,
				dagFinishTs, queueName);
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}

	public String getDagId() {
		return dagId;
	}

	public void setDagId(String dagId) {
		this.dagId = dagId;
	}

	public String getDagName() {
		return dagName;
	}

	public void setDagName(String dagName) {
		this.dagName = dagName;
	}

	public String getCallerId() {
		return callerId;
	}

	public void setCallerId(String callerId) {
		this.callerId = callerId;
	}

	public String getCallerType() {
		return callerType;
	}

	public void setCallerType(String callerType) {
		this.callerType = callerType;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public String getDagSubmitTs() {
		return dagSubmitTs;
	}

	public void setDagSubmitTs(String dagSubmitTs) {
		this.dagSubmitTs = dagSubmitTs;
	}

	public String getDagInitTs() {
		return dagInitTs;
	}

	public void setDagInitTs(String dagInitTs) {
		this.dagInitTs = dagInitTs;
	}

	public String getDagStartTs() {
		return dagStartTs;
	}

	public void setDagStartTs(String dagStartTs) {
		this.dagStartTs = dagStartTs;
	}

	public String getDagFinishTs() {
		return dagFinishTs;
	}

	public void setDagFinishTs(String dagFinishTs) {
		this.dagFinishTs = dagFinishTs;
	}
	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}
		private String appId;
		private String dagId;
		private String dagName;
		private String callerId;
		private String callerType;
		private String description;
		private String dagSubmitTs;
		private String dagInitTs;
		private String dagStartTs;
		private String dagFinishTs;
		private String queueName;


}
