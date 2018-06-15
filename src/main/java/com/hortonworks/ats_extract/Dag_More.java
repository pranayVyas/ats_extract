package com.hortonworks.ats_extract;

/**
 * Dag_More.java - This contains the data model for dag more file
 * 
 * @author Pranay Ashok Vyas
 * @version 1.0
 * @see 
 * @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
 *            licensed to you pursuant to the written agreement between
 *            Hortonworks and your company. If no such written agreement exists,
 *            you do not have a license to this software
 */

public class Dag_More {
	@Override
	public String toString() {
		return String.format(
				"Dag_More [dagId=%s, tableName=%s, recordsWritten=%s, memoryRequestedGB=%s, memoryUsedGB=%s, cpuMilliseconds=%s, succeededTasks=%s, failedTasks=%s, numVertices=%s, hdfsReadOps=%s, hdfsWriteOps=%s, hdfsCreateOps=%s, dagDuration=%s, filesWritten=%s]",
				dagId, tableName, recordsWritten, memoryRequestedGB, memoryUsedGB, cpuMilliseconds, succeededTasks,
				failedTasks, numVertices, hdfsReadOps, hdfsWriteOps, hdfsCreateOps, dagDuration, filesWritten);
	}

	public String getDagId() {
		return dagId;
	}

	public void setDagId(String dagId) {
		this.dagId = dagId;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public long getRecordsWritten() {
		return recordsWritten;
	}

	public void setRecordsWritten(long recordsWritten) {
		this.recordsWritten = recordsWritten;
	}

	public long getMemoryRequestedGB() {
		return memoryRequestedGB;
	}

	public void setMemoryRequestedGB(long memoryRequestedGB) {
		this.memoryRequestedGB = memoryRequestedGB;
	}

	public long getMemoryUsedGB() {
		return memoryUsedGB;
	}

	public void setMemoryUsedGB(long memoryUsedGB) {
		this.memoryUsedGB = memoryUsedGB;
	}

	public long getCpuMilliseconds() {
		return cpuMilliseconds;
	}

	public void setCpuMilliseconds(long cpuMilliseconds) {
		this.cpuMilliseconds = cpuMilliseconds;
	}

	public long getSucceededTasks() {
		return succeededTasks;
	}

	public void setSucceededTasks(long succeededTasks) {
		this.succeededTasks = succeededTasks;
	}

	public long getFailedTasks() {
		return failedTasks;
	}

	public void setFailedTasks(long failedTasks) {
		this.failedTasks = failedTasks;
	}

	public long getNumVertices() {
		return numVertices;
	}

	public void setNumVertices(long numVertices) {
		this.numVertices = numVertices;
	}

	public long getHdfsReadOps() {
		return hdfsReadOps;
	}

	public void setHdfsReadOps(long hdfsReadOps) {
		this.hdfsReadOps = hdfsReadOps;
	}

	public long getHdfsWriteOps() {
		return hdfsWriteOps;
	}

	public void setHdfsWriteOps(long hdfsWriteOps) {
		this.hdfsWriteOps = hdfsWriteOps;
	}

	public long getHdfsCreateOps() {
		return hdfsCreateOps;
	}

	public void setHdfsCreateOps(long hdfsCreateOps) {
		this.hdfsCreateOps = hdfsCreateOps;
	}

	public long getDagDuration() {
		return dagDuration;
	}

	public void setDagDuration(long dagDuration) {
		this.dagDuration = dagDuration;
	}

	public long getFilesWritten() {
		return filesWritten;
	}

	public void setFilesWritten(long filesWritten) {
		this.filesWritten = filesWritten;
	}

	private String dagId;
	private String tableName;
	private long recordsWritten;
	private long memoryRequestedGB;
	private long memoryUsedGB;
	private long cpuMilliseconds;
	private long succeededTasks;
	private long failedTasks;
	private long numVertices;
	private long hdfsReadOps;
	private long hdfsWriteOps;
	private long hdfsCreateOps;
	private long dagDuration;
	private long filesWritten;

}
