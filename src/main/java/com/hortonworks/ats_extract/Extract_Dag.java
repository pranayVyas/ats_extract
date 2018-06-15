package com.hortonworks.ats_extract;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

/**
* Extract_Dag.java - This class parses dag level details from json files
* 
* @author Pranay Ashok Vyas
* @version 1.0
* @see HP Inc.
* @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
*            licensed to you pursuant to the written agreement between
*            Hortonworks and your company. If no such written agreement exists,
*            you do not have a license to this software
*/

public class Extract_Dag {

	// Set Log4J
	static Logger logger = Logger.getLogger(Extract_Dag.class);

	private App_Detail app_detail_local = null; // read only
	private Dag_Detail dag_detail_local = null; // write
	private Dag_More dag_more_local = null; // write
	private Vertices vertices_local = null; // write

	private String dag_id = "";
	private String prev_dag_id = null;
	private long epochdagsubmit = 0;

	public Extract_Dag() {
		dag_detail_local = new Dag_Detail();
		dag_more_local = new Dag_More();
		vertices_local = new Vertices();
		// new HashMap<String, Dag_Detail>();
		// new HashMap<String, Dag_More>();
		// new HashMap<String, Vertices>();
	}

	public void parseDagDetails(List<JSONObject> list_dag_json, App_Detail app_detail,
			Map<String, Dag_Detail> dag_dtl_map, Map<String, Dag_More> dag_more_map,
			Map<String, Vertices> vertices_map) {
		try {

			app_detail_local = app_detail;
			Iterator<JSONObject> itr1 = list_dag_json.iterator();

			while (itr1.hasNext()) {
				JSONObject job = itr1.next();

				dag_id = job.optString("entity");

				if (!dag_id.equalsIgnoreCase(prev_dag_id) && (prev_dag_id != null)) {
					// write previous to datamodel and current to new datamodel										
					if (dag_detail_local.getCallerId().equals(job.optJSONObject("otherinfo").optString("callerId"))){						
						// this will happen if the previous dag did not have dag finish task. 
						// this is possible when File Merge is triggered. Take the start time of file merge
						// as dagfinish time for prev_dag_id. 
						logger.debug("dagFinish not found");
						logger.debug(job);						
							JSONArray jsonarray = job.getJSONArray("events");
							logger.debug(epochdagsubmit + "  " + jsonarray.optJSONObject(0).getLong("timestamp"));
							dag_detail_local.setDagFinishTs(new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
									.format(new java.util.Date(jsonarray.optJSONObject(0).getLong("timestamp"))));		
							dag_more_local.setDagDuration(jsonarray.optJSONObject(0).getLong("timestamp") - epochdagsubmit);													
					}
					dag_dtl_map.put(prev_dag_id, dag_detail_local);
					dag_more_map.put(prev_dag_id, dag_more_local);
					// vertices_map.put(vertices_local.getVertexId(),
					// vertices_local);
					dag_detail_local = new Dag_Detail();
					dag_more_local = new Dag_More();
					// dag_detail_local = new Dag_Detail();
				}
				dag_detail_local.setDagId(dag_id);
				dag_more_local.setDagId(dag_id);

				prev_dag_id = dag_id;

				JSONArray jsonarray = job.getJSONArray("events");
				String eventtype = jsonarray.optJSONObject(0).optString("eventtype");
				String event_time = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
						.format(new java.util.Date(jsonarray.optJSONObject(0).getLong("timestamp")));

				switch (eventtype.toUpperCase()) {
				case "DAG_SUBMITTED":
					logger.debug("Processing Submit for  " + dag_id);
					epochdagsubmit = jsonarray.optJSONObject(0).getLong("timestamp");
					processDagSubmit(event_time, job);
					break;
				case "DAG_INITIALIZED":
					logger.debug("Processing Init for  " + dag_id);
					processDagInit(event_time, job, vertices_map);
					break;
				case "DAG_STARTED":
					logger.debug("Processing Start for  " + dag_id);
					processDagStart(event_time, job);
					break;
				case "DAG_FINISHED":
					logger.debug("Processing Finish for  " + dag_id);
					processDagFinish(event_time, job);
					break;
				default:
					break;
				}
			}
			if (!itr1.hasNext()) {
				// write previous to datamodel and current to new datamodel			
				dag_dtl_map.put(prev_dag_id, dag_detail_local);
				dag_more_map.put(prev_dag_id, dag_more_local);
				dag_detail_local = new Dag_Detail();
				dag_more_local = new Dag_More();
			}
		} catch (Exception e) {

		}

	}

	private void processDagFinish(String event_time, JSONObject job) {

		logger.debug("processDagFinish 1- dag_detail_local - " + dag_more_local + " "+ job);
		dag_detail_local.setAppId(app_detail_local.getAppId());
		dag_detail_local.setDagFinishTs(event_time);
		extractCounter(job.optJSONObject("otherinfo").optJSONObject("counters").optJSONArray("counterGroups"));
		dag_more_local.setFailedTasks(job.optJSONObject("otherinfo").optLong("numFailedTasks"));
		logger.debug(job.optJSONObject("otherinfo").optLong("numFailedTasks"));
		dag_more_local.setSucceededTasks(job.optJSONObject("otherinfo").optLong("numSucceededTasks"));
		dag_more_local.setDagDuration(job.optJSONObject("otherinfo").optLong("timeTaken"));
		long memRequested = (dag_more_local.getSucceededTasks()+ dag_more_local.getFailedTasks()) * (app_detail_local.getContainerSize()/1024);
		dag_more_local.setMemoryRequestedGB(memRequested);
		logger.debug(job.optJSONObject("otherinfo").optLong("timeTaken"));
		logger.debug("processDagFinish 2 - dag_detail_local - " + dag_more_local);
	}

	private void extractCounter(JSONArray counter_array) {

		for (int i = 0; i < counter_array.length(); i++) {
			switch (counter_array.optJSONObject(i).optString("counterGroupName")) {
			case "org.apache.tez.common.counters.TaskCounter":
				extractTaskCounter(counter_array.optJSONObject(i).optJSONArray("counters"));
				break;
			case "org.apache.tez.common.counters.FileSystemCounter":
				extractFileCounter(counter_array.optJSONObject(i).optJSONArray("counters"));
				break;
			case "HIVE":
				extractHiveCounter(counter_array.optJSONObject(i).optJSONArray("counters"));
				break;
			default:
				break;
			}
		}
	}

	private void extractHiveCounter(JSONArray optJSONArray) {
		// TODO Auto-generated method stub
		for (int i = 0; i < optJSONArray.length(); i++) {
			logger.debug(optJSONArray.optJSONObject(i).optLong("counterValue"));
			switch (optJSONArray.optJSONObject(i).optString("counterName")) {

			case "CREATED_FILES":
				dag_more_local.setFilesWritten(optJSONArray.optJSONObject(i).optLong("counterValue"));
				
				break;
			default:
				String counter_name = optJSONArray.optJSONObject(i).optString("counterName");
				String prefix = "RECORDS_OUT_1_";
				if (counter_name.startsWith("RECORDS_OUT_1_")) {
					dag_more_local.setRecordsWritten(optJSONArray.optJSONObject(i).optLong("counterValue"));
					dag_more_local.setTableName(counter_name.substring(prefix.length(), counter_name.length()));
				}
				break;
			}
			// System.out.println(dag_more_local.toString());
		}
	}

	private void extractFileCounter(JSONArray optJSONArray) {
		// TODO Auto-generated method stub
		for (int i = 0; i < optJSONArray.length(); i++) {
			logger.debug(optJSONArray.optJSONObject(i).optLong("counterValue"));
			switch (optJSONArray.optJSONObject(i).optString("counterName")) {
			case "HDFS_READ_OPS":
				dag_more_local.setHdfsReadOps(optJSONArray.optJSONObject(i).optLong("counterValue"));
				break;
			case "HDFS_WRITE_OPS":
				dag_more_local.setHdfsWriteOps(optJSONArray.optJSONObject(i).optLong("counterValue"));
				break;
			case "dag_more_local":
				dag_more_local.setHdfsCreateOps(optJSONArray.optJSONObject(i).optLong("counterValue"));
				break;
			default:
				break;
			}
		}
	}

	private void extractTaskCounter(JSONArray optJSONArray) {
		// TODO Auto-generated method stub
		for (int i = 0; i < optJSONArray.length(); i++) {
			switch (optJSONArray.optJSONObject(i).optString("counterName")) {
			case "CPU_MILLISECONDS":
				dag_more_local.setCpuMilliseconds(optJSONArray.optJSONObject(i).optLong("counterValue"));
				break;
			case "COMMITTED_HEAP_BYTES":
				dag_more_local.setMemoryUsedGB((Math.round(optJSONArray.optJSONObject(i).optLong("counterValue")/1024/1024/1024)));
				break;
			default:
				break;
			}
		}
	}

	private void processDagStart(String event_time, JSONObject job) {
		dag_detail_local.setAppId(app_detail_local.getAppId());
		dag_detail_local.setDagStartTs(event_time);
	}

	private void processDagInit(String event_time, JSONObject job, Map<String, Vertices> vertices_map) {

		dag_detail_local.setAppId(app_detail_local.getAppId());
		dag_detail_local.setDagInitTs(event_time);
		//getVertex(job.optJSONObject("otherinfo").optJSONObject("vertexNameIdMapping"), vertices_map);
	}

	private void getVertex(JSONObject vertex_json, Map<String, Vertices> vertices_map) {
		// TODO to be written later
		Iterator<?> keys = vertex_json.keys();
		dag_more_local.setNumVertices(vertex_json.length());
		while (keys.hasNext()) {
			String key = (String) keys.next();
			try {
				vertices_local = new Vertices();
				vertices_local.setDagId(dag_detail_local.getDagId());
				vertices_local.setVertexId(vertex_json.get(key).toString());
				vertices_local.setVertexName(vertex_json.get(key).toString());
				vertices_map.put(vertices_local.getVertexId(), vertices_local);
			} catch (JSONException e) {
				logger.error(e);
				e.printStackTrace();
			}
		}

	}

	private void processDagSubmit(String event_time, JSONObject job) {
		// get submissiontime
		//logger.debug("Inside processdagsubmit - event_time = " + event_time+ "\nJSONObject = " + job+"\n dag detail = "+ dag_detail_local);
		try {
			dag_detail_local.setAppId(app_detail_local.getAppId());
			dag_detail_local.setDagSubmitTs(event_time);
			dag_detail_local.setCallerId(job.optJSONObject("otherinfo").optString("callerId"));
			dag_detail_local.setCallerType(job.optJSONObject("otherinfo").optString("callerType"));
			String description = job.optJSONObject("otherinfo").optJSONObject("dagPlan").optJSONObject("dagContext")
					.optString("description");
			description = description.replaceAll("\\r", " ").replaceAll("\\n", " ");
			dag_detail_local.setDescription(description.replaceAll("\r", " ").replaceAll("\n", " "));
			dag_detail_local.setDagName(job.optJSONObject("otherinfo").optJSONObject("dagPlan").optString("dagName"));
			JSONArray queuearray = job.optJSONObject("primaryfilters").optJSONArray("queueName");
			if (queuearray != null){
				dag_detail_local.setQueueName(queuearray.optString(0));
			}			
			getEdges(job.optJSONObject("otherinfo").optJSONObject("dagPlan").optJSONArray("edges"));
			//logger.debug("After processdagsubmit - dag_detail_local = " + dag_detail_local.toString());
		} catch (Exception e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	private void getEdges(JSONArray jsonArray) {
		// TODO parse edges yet to be implemented
		/*
		 * if (jsonArray != null) { for (int i = 0; i < jsonArray.length(); i++)
		 * { System.out.println(dag_detail_local.getDagId() + " " +
		 * jsonArray.optJSONObject(i));
		 * 
		 * } }
		 */
	}

}
