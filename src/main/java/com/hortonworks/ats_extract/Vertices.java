package com.hortonworks.ats_extract;

public class Vertices {

	@Override
	public String toString() {
		return String.format(
				"Vertices [vertexName=%s, vertexId=%s, dagId=%s, vertexSucceededTasks=%s, vertexFailedTasks=%s, vertexAvgtaskDuration=%s, vertexMintaskDuration=%s, vertexMaxtaskDuration=%s, vertexRunDuration=%s, vertexStartTs=%s, vertexEndTs=%s, vertexStatus=%s]",
				vertexName, vertexId, dagId, vertexSucceededTasks, vertexFailedTasks, vertexAvgtaskDuration,
				vertexMintaskDuration, vertexMaxtaskDuration, vertexRunDuration, vertexStartTs, vertexEndTs,
				vertexStatus);
	}

	public String getVertexName() {
		return vertexName;
	}

	public void setVertexName(String vertexName) {
		this.vertexName = vertexName;
	}

	public String getVertexId() {
		return vertexId;
	}

	public void setVertexId(String vertexId) {
		this.vertexId = vertexId;
	}

	public String getDagId() {
		return dagId;
	}

	public void setDagId(String dagId) {
		this.dagId = dagId;
	}

	private String vertexName;
	private String vertexId;
	private String dagId;
	private long vertexSucceededTasks;
	private long vertexFailedTasks;
	private long vertexAvgtaskDuration;
	private long vertexMintaskDuration;
	private long vertexMaxtaskDuration;
	private long vertexRunDuration;
	private String vertexStartTs;
	private String vertexEndTs;
	private String vertexStatus;

	public long getVertexSucceededTasks() {
		return vertexSucceededTasks;
	}

	public void setVertexSucceededTasks(long vertexSucceededTasks) {
		this.vertexSucceededTasks = vertexSucceededTasks;
	}

	public long getVertexFailedTasks() {
		return vertexFailedTasks;
	}

	public void setVertexFailedTasks(long vertexFailedTasks) {
		this.vertexFailedTasks = vertexFailedTasks;
	}

	public long getVertexAvgtaskDuration() {
		return vertexAvgtaskDuration;
	}

	public void setVertexAvgtaskDuration(long vertexAvgtaskDuration) {
		this.vertexAvgtaskDuration = vertexAvgtaskDuration;
	}

	public long getVertexMintaskDuration() {
		return vertexMintaskDuration;
	}

	public void setVertexMintaskDuration(long vertexMintaskDuration) {
		this.vertexMintaskDuration = vertexMintaskDuration;
	}

	public long getVertexMaxtaskDuration() {
		return vertexMaxtaskDuration;
	}

	public void setVertexMaxtaskDuration(long vertexMaxtaskDuration) {
		this.vertexMaxtaskDuration = vertexMaxtaskDuration;
	}

	public long getVertexRunDuration() {
		return vertexRunDuration;
	}

	public void setVertexRunTs(long vertexRunTs) {
		this.vertexRunDuration = vertexRunTs;
	}

	public String getVertexStartTs() {
		return vertexStartTs;
	}

	public void setVertexStartTs(String vertexStartTs) {
		this.vertexStartTs = vertexStartTs;
	}

	public String getVertexEndTs() {
		return vertexEndTs;
	}

	public void setVertexEndTs(String vertexEndTs) {
		this.vertexEndTs = vertexEndTs;
	}

	public String getVertexStatus() {
		return vertexStatus;
	}

	public void setVertexStatus(String vertexStatus) {
		this.vertexStatus = vertexStatus;
	}
}
