package com.hortonworks.ats_extract;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
* Extract_App.java - This class parses application level details from json files
* 
* @author Pranay Ashok Vyas
* @version 1.0
* @see HP Inc.
* @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
*            licensed to you pursuant to the written agreement between
*            Hortonworks and your company. If no such written agreement exists,
*            you do not have a license to this software
*/

public class Extract_App {

	// Set log4J
	static Logger logger = Logger.getLogger(Extract_App.class);

	// Set Static Objects

	public Extract_App() {

	}

	private void parseAttDetails(JSONObject job, App_Detail app_detail) {
		logger.debug("Inside parseAttDetails for - " + job);
		try {
			JSONArray jsonarray = job.getJSONArray("events");
			String eventtype = jsonarray.getJSONObject(0).getString("eventtype");
			String ts = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
					.format(new java.util.Date(jsonarray.getJSONObject(0).getLong("timestamp")));
			if (eventtype.equalsIgnoreCase("AM_LAUNCHED")) {
				app_detail.setAmLaunchTs(ts);
				app_detail.setAttId(job.getJSONObject("otherinfo").getString("applicationAttemptId"));
			}
			if (eventtype.equalsIgnoreCase("AM_STARTED")) {
				app_detail.setAmStartTs(ts);
			}

		} catch (JSONException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	public void parseAppDetails(List<JSONObject> list_app_json, App_Detail app_detail, Dag_Detail dag_detail) {
		logger.debug("Inside parseAppDetails Method");
		int attcount = 0;
		try {
			
			Iterator<JSONObject> itr = list_app_json.iterator();
			while (itr.hasNext()) {
				JSONObject job = itr.next();
				if (job.getString("entitytype").equals("TEZ_APPLICATION")) {
					app_detail.setAppId(job.getJSONObject("otherinfo").getString("applicationId"));
					app_detail.setUserId(job.getJSONObject("otherinfo").getString("user"));
					String containerSize = job.optJSONObject("otherinfo").optJSONObject("config").optString("hive.tez.container.size");
					long containerLong = 0;
					if (containerSize == "") {containerLong = 0;} else {containerLong = Long.parseLong(containerSize);};
					app_detail.setContainerSize(containerLong);
				} else {
					attcount++;
					parseAttDetails(job, app_detail);
				}
			}
		} catch (JSONException e) {
			logger.error(e);
			e.printStackTrace();
		}
		
		app_detail.setAttCount(attcount);
	}

}
