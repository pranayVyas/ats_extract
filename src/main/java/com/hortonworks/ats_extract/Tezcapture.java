package com.hortonworks.ats_extract;

import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.log.Log;

/**
 * Tezcapture.java - This is the main class called to extract ats json files 
 * 
 * @author Pranay Ashok Vyas
 * @version 1.0
 * @see 
 * @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
 *            licensed to you pursuant to the written agreement between
 *            Hortonworks and your company. If no such written agreement exists,
 *            you do not have a license to this software
 */

public class Tezcapture {
	
	
	
	static Logger logger = Logger.getLogger(Tezcapture.class);
	private static Configuration cf = null;
	private static FileSystem fs = null;
	static String directorylvl = "/etc/hadoop/conf/";
	// private String directorylvl =
	// "/Users/pranayashokvyas/Documents/workspace/config/";
	private String log_level;
	private Extract_App ext_app = null;
	private Extract_Dag ext_dag = null;
	private App_Detail app_detail = null;
	private Dag_Detail dag_detail = null;
	private Vertices vertices = null;
	private Map<String, App_Detail> app_dtl_map = null;
	private Map<String, Dag_Detail> dag_dtl_map = null;
	private Map<String, Dag_More> dag_more_map = null;
	private Map<String, Vertices> vertices_map = null;
	private List<Path> vertices_path = null;
	private Vertices vertices_rewrite = null;
	private static boolean skipvertices = false;
	private static Post_Metrics write_out = null;
	// Define Data Models

	public static void main(String[] args) {

		Tezcapture tezcapture = new Tezcapture();

		// Accept log level parameter. Default level is INFO.
		int cnt = 0;
		if (args.length >= 3) {
			tezcapture.log_level = "INFO";
			if (args[0].equalsIgnoreCase("DEBUG")) {
				tezcapture.log_level = "DEBUG";
			}
			tezcapture.setuplog4j();
			if (args.length == 4) {
				if (args[3].equalsIgnoreCase("true")) {
					skipvertices = true;
					logger.info("vertices processing will be slipped");
				} else {
					logger.warn("vertices processing not skipped. This will take time to process");
				}
			}
			long startTime = 0;
			long endTime = 0;
			try {
				startTime = new java.text.SimpleDateFormat("yyyy-MM-dd").parse(args[1]).getTime();
				endTime = new java.text.SimpleDateFormat("yyyy-MM-dd").parse(args[2]).getTime();

			} catch (ParseException e) {
				e.printStackTrace();
			}

			logger.info("FILES WITH MODIFIED TIME BETWEEN " + startTime + " and " + endTime + " WILL BE PROCESSED");

			// Create HDFS Configuration and File System
			tezcapture.setupconn();

			List<Path> ats_summary = new ArrayList<Path>();

			/*
			 * These are hashmap that stores key, object. Objects are defined in
			 * its own class files. App Detail key is applicationId Dag Detail
			 * key is DagId Dag More key is DagId Vertices key is Vertex Id
			 */
			tezcapture.app_dtl_map = new HashMap<String, App_Detail>();
			tezcapture.dag_dtl_map = new HashMap<String, Dag_Detail>();
			tezcapture.dag_more_map = new HashMap<String, Dag_More>();
			tezcapture.vertices_map = new HashMap<String, Vertices>();

			ats_summary = tezcapture.extract_ats_summary_path(startTime, endTime);
			write_out = new Post_Metrics();
			if (skipvertices) {
				Iterator<Path> atsitr = ats_summary.iterator();
				while (atsitr.hasNext()) {
					Path path = atsitr.next();
					tezcapture.parse_log(path);
					cnt++;
					logger.info("TOTAL FILES PROCESSED - " + cnt);
				}

				write_out.WriteAppDagFiles(tezcapture.app_dtl_map, tezcapture.dag_more_map, tezcapture.dag_dtl_map);
				tezcapture.app_dtl_map.clear();
				tezcapture.dag_more_map.clear();
				tezcapture.dag_dtl_map.clear();
			}
			cnt = 0;
			if (!skipvertices) {
				tezcapture.vertices_rewrite = new Vertices();
				write_out.createVerticesFile();
				Iterator<Path> verItr = tezcapture.vertices_path.iterator();
				while (verItr.hasNext()) {
					cnt++;
					Path path = verItr.next();
					tezcapture.getVerticesFiles(path);
					Log.debug(tezcapture.vertices_rewrite.toString());
					// write_out.writeVertices(tezcapture.vertices_rewrite);
					logger.info("TOTAL DAG-VERTEX PROCESSED - " + cnt);
				}
			}
		} else {
			logger.info("Invalid input arguments provided - " + args.toString() + " " + args.length);
		}
	}

	private void parse_log(Path next) {
		Log.debug("Inside Main Parse_log");
		try {
			InputStream inp_stream = null;
			Scanner sc = null;
			List<JSONObject> list_app_json = null;
			List<JSONObject> list_dag_json = null;
			inp_stream = fs.open(next);

			sc = new Scanner(inp_stream, "UTF-8");
			list_app_json = new ArrayList<JSONObject>();
			list_dag_json = new ArrayList<JSONObject>();

			ext_app = new Extract_App();
			ext_dag = new Extract_Dag();

			while (sc.hasNextLine()) {

				String line = sc.nextLine();

				JSONObject job = new JSONObject(line);
				// logger.debug(job);

				if ((job.getString("entitytype").equals("TEZ_APPLICATION"))
						|| (job.getString("entitytype").equals("TEZ_APPLICATION_ATTEMPT"))) {

					list_app_json.add(job);

				}
				if (job.getString("entitytype").equals("TEZ_DAG_ID")) {
					String pattern = "\\{\"events\":";
					logger.debug(pattern);
					String[] linearray = line.split(pattern);

					if (linearray.length > 2) {
						logger.debug(linearray[1]);
						logger.debug(linearray[2]);
						JSONObject job1 = new JSONObject("{\"events\":".concat(linearray[1]));
						JSONObject job2 = new JSONObject("{\"events\":".concat(linearray[2]));
						list_dag_json.add(job1);
						list_dag_json.add(job2);
						logger.debug(job1.toString());
						logger.debug(job2.toString());
					} else {
						list_dag_json.add(job);
					}
				}
			}
			app_detail = new App_Detail();
			dag_detail = new Dag_Detail();
			vertices = new Vertices();

			ext_app.parseAppDetails(list_app_json, app_detail, dag_detail);
			ext_dag.parseDagDetails(list_dag_json, app_detail, dag_dtl_map, dag_more_map, vertices_map);

			app_dtl_map.put(app_detail.getAppId(), app_detail);

			list_app_json.clear();
			list_dag_json.clear();
			sc.close();

		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		} catch (JSONException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

	private void getVerticesFiles(Path path) {
		Log.debug("Extracting Vertices");
		InputStream inp_stream = null;
		Scanner sc = null;
		try {
			if (fs.exists(path)) {
				inp_stream = fs.open(path);
				sc = new Scanner(inp_stream, "UTF-8");
				while (sc.hasNextLine()) {
					try {
						JSONObject jobVertices = new JSONObject(sc.nextLine());

						if ((jobVertices.optString("entitytype").equals("TEZ_VERTEX_ID"))
								&& (jobVertices.optJSONArray("events").optJSONObject(0).optString("eventtype")
										.equals("VERTEX_FINISHED"))) {
							long vertexSucceededTasks = 0;
							long vertexFailedTasks = 0;
							long vertexAvgtaskDuration = 0;
							long vertexMintaskDuration = 0;
							long vertexMaxtaskDuration = 0;
							long vertexRunTs = 0;
							String vertexName = "";
							String vertexDagId = "";
							String vertexStartTs = "";
							String vertexEndTs = "";
							String vertexStatus = "";

							vertexSucceededTasks = (jobVertices.optJSONObject("otherinfo")
									.optLong("numSucceededTasks"));
							vertexName = (jobVertices.optJSONObject("otherinfo").optString("vertexName"));
							JSONArray dagArray = jobVertices.optJSONObject("primaryfilters").optJSONArray("TEZ_DAG_ID");
							if (dagArray != null) {
								vertexDagId = dagArray.optString(0);
							}
							vertexName = (jobVertices.optJSONObject("otherinfo").optString("vertexName"));
							JSONObject statsJobj = new JSONObject();
							statsJobj = jobVertices.optJSONObject("otherinfo").optJSONObject("stats");
							vertexAvgtaskDuration = statsJobj.optLong("avgTaskDuration");
							vertexMaxtaskDuration = statsJobj.optLong("maxTaskDuration");
							vertexMintaskDuration = statsJobj.optLong("minTaskDuration");
							vertexStartTs = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
									.format(new java.util.Date(statsJobj.optLong("firstTaskStartTime")));

							vertexEndTs = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
									.format(new java.util.Date(statsJobj.getLong("lastTaskFinishTime")));
							vertexStatus = jobVertices.optJSONObject("otherinfo").optString("status");
							vertexRunTs = jobVertices.optJSONObject("otherinfo").optLong("timeTaken");
							String vertexId = jobVertices.optString("entity");
							vertices_rewrite.setVertexAvgtaskDuration(vertexAvgtaskDuration);
							vertices_rewrite.setVertexId(vertexId);
							vertices_rewrite.setDagId(vertexDagId);
							vertices_rewrite.setVertexName(vertexName);
							vertices_rewrite.setVertexEndTs(vertexEndTs);
							vertices_rewrite.setVertexFailedTasks(vertexFailedTasks);
							vertices_rewrite.setVertexMaxtaskDuration(vertexMaxtaskDuration);
							vertices_rewrite.setVertexMintaskDuration(vertexMintaskDuration);
							vertices_rewrite.setVertexRunTs(vertexRunTs);
							vertices_rewrite.setVertexStartTs(vertexStartTs);
							vertices_rewrite.setVertexStatus(vertexStatus);
							vertices_rewrite.setVertexSucceededTasks(vertexSucceededTasks);
							Log.debug(vertices_rewrite.toString());
							write_out.writeVertices(vertices_rewrite);
							vertices_rewrite = new Vertices();
						}
					} catch (JSONException e) {
						logger.error(e);
					}
				}
			} else {
				logger.warn("file does not exist - " + path);
			}
		} catch (IOException e) {
			logger.error(e);
		}

	}

	private List<Path> extract_ats_summary_path(long startTime, long endTime) {

		Log.debug("Extracting ATS Files - ");
		Path ats_path = new Path("/ats/done/");
		List<Path> summary_path = new ArrayList<Path>();
		vertices_path = new ArrayList<Path>();
		int cnt = 0;
		try {
			if (fs.exists(ats_path)) {
				RemoteIterator<LocatedFileStatus> rs_file = fs.listFiles(ats_path, true);
				while (rs_file.hasNext()) {
					LocatedFileStatus lfs = rs_file.next();
					Path ps = lfs.getPath();
					long modifyTime = lfs.getModificationTime();
					if ((modifyTime >= startTime) && (modifyTime < endTime)) {

						if (ps.getName().startsWith("summarylog")) {
							summary_path.add(ps);
							cnt++;
						}
						if ((ps.getName().startsWith("entitylog-timelineEntityGroupId"))
								&& (ps.getName().contains("dag"))) {
							vertices_path.add(ps);
						}
					}
				}
				logger.info("TOTAL FILES TO PROCESS - " + cnt);
			}
		} catch (IOException e) {
			logger.info("ATS Path not found", e);
			e.printStackTrace();
		}
		return summary_path;
	}

	private void setuplog4j() {

		Logger root = Logger.getRootLogger();
		PatternLayout layout = new PatternLayout("%d{ISO8601} [%t] %-5p %c %M %x - %m%n");
		root.setLevel(Level.INFO);
		if (log_level.length() > 0) {
			if (log_level.equalsIgnoreCase("debug")) {
				root.setLevel(Level.DEBUG);
			}
		}
		root.addAppender(new ConsoleAppender(layout));
	}

	private void setupconn() {

		Log.debug("setting up connection with configurations from - " + directorylvl);
		cf = new Configuration();

		cf.addResource(new Path(directorylvl + "hdfs-site.xml"));
		cf.addResource(new Path(directorylvl + "core-site.xml"));
		cf.addResource(new Path(directorylvl + "yarn-site.xml"));
		try {
			fs = FileSystem.get(cf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.info("Filesystem creation failed", e);
			e.printStackTrace();
		}

	}

}
