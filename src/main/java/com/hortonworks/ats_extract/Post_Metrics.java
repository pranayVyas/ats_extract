package com.hortonworks.ats_extract;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.log4j.Logger;

/**
* Post_Metrics.java - This class writes csv files for captures metrics
* 
* @author Pranay Ashok Vyas
* @version 1.0
* @see 
* @Copyright © 2018 Hortonworks Inc. All Rights Reserved. This software/code is
*            licensed to you pursuant to the written agreement between
*            Hortonworks and your company. If no such written agreement exists,
*            you do not have a license to this software
*/

public class Post_Metrics {

	// Set log4J
	static Logger logger = Logger.getLogger(Post_Metrics.class);

	// Create File writer components
	private BufferedWriter appWriter = null;
	private BufferedWriter dagDtlWriter = null;
	private BufferedWriter dagmoreWriter = null;
	private BufferedWriter verticesWriter = null;
	private CSVPrinter csvPrinterApp = null;
	private CSVPrinter csvPrinterDagDtl = null;
	private CSVPrinter csvPrinterDagMore = null;
	private CSVPrinter csvPrinterVertices = null;

	public Post_Metrics() {
		// Initialization Constructor
	}

	public void WriteAppDagFiles(Map<String, App_Detail> app_dtl_map, Map<String, Dag_More> dag_more_map,
			Map<String, Dag_Detail> dag_dtl_map) {

		logger.info("Beginning to write AppDetail file");

		File directory = new File(".");
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		Date date = new Date();
		String dateAppend = dateFormat.format(date);

		try {
			appWriter = Files
					.newBufferedWriter(Paths.get(directory.getCanonicalPath() + "/AppMetric-" + dateAppend + ".csv"));
			csvPrinterApp = new CSVPrinter(appWriter, CSVFormat.DEFAULT.withHeader("appId", "attId", "userId",
					"amLaunchTs", "amStartTs", "attCount", "containerSize"));

			dagDtlWriter = Files.newBufferedWriter(
					Paths.get(directory.getCanonicalPath() + "/DagDtlMetric-" + dateAppend + ".csv"));
			csvPrinterDagDtl = new CSVPrinter(dagDtlWriter,
					CSVFormat.DEFAULT.withHeader("appId", "dagId", "dagName", "callerId", "callerType", "description",
							"dagSubmitTs", "dagInitTs", "dagStartTs", "dagFinishTs", "queueName"));
			dagmoreWriter = Files.newBufferedWriter(
					Paths.get(directory.getCanonicalPath() + "/DagMoreMetric-" + dateAppend + ".csv"));
			csvPrinterDagMore = new CSVPrinter(dagmoreWriter,
					CSVFormat.DEFAULT.withHeader("dagId", "tableName", "recordsWritten", "memoryRequestedGB",
							"memoryUsedGB", "cpuMilliseconds", "succeededTasks", "failedTasks", "numVertices",
							"hdfsReadOps", "hdfsWriteOps", "hdfsCreateOps", "dagDuration", "filesWritten"));
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}

		for (String key : app_dtl_map.keySet()) {
			App_Detail app_detail = new App_Detail();
			app_detail = app_dtl_map.get(key);

			try {
				csvPrinterApp.printRecord(app_detail.getAppId(), app_detail.getAttId(), app_detail.getUserId(),
						app_detail.getAmLaunchTs(), app_detail.getAmStartTs(), app_detail.getAttCount(),
						app_detail.getContainerSize());
				csvPrinterApp.flush();
			} catch (IOException e) {
				logger.error(e);
				e.printStackTrace();
			}
		}
		logger.info("Beginning to write DagDetail file");
		for (String key : dag_dtl_map.keySet()) {
			Dag_Detail dag_detail = new Dag_Detail();
			dag_detail = dag_dtl_map.get(key);

			try {
				csvPrinterDagDtl.printRecord(dag_detail.getAppId(), dag_detail.getDagId(),
						"\"" + dag_detail.getDagName() + "\"", dag_detail.getCallerId(), dag_detail.getCallerType(),
						"\"" + dag_detail.getDescription() + "\"", dag_detail.getDagSubmitTs(),
						dag_detail.getDagInitTs(), dag_detail.getDagStartTs(), dag_detail.getDagFinishTs(),
						dag_detail.getQueueName());
				csvPrinterDagDtl.flush();
			} catch (IOException e) {
				logger.error(e);
				e.printStackTrace();
			}
		}
		logger.info("Beginning to write DagMore file");
		for (String key : dag_more_map.keySet()) {
			Dag_More dag_more = new Dag_More();
			dag_more = dag_more_map.get(key);

			try {
				csvPrinterDagMore.printRecord(dag_more.getDagId(), dag_more.getTableName(),
						dag_more.getRecordsWritten(), dag_more.getMemoryRequestedGB(), dag_more.getMemoryUsedGB(),
						dag_more.getCpuMilliseconds(), dag_more.getSucceededTasks(), dag_more.getFailedTasks(),
						dag_more.getNumVertices(), dag_more.getHdfsReadOps(), dag_more.getHdfsWriteOps(),
						dag_more.getHdfsCreateOps(), dag_more.getDagDuration(), dag_more.getFilesWritten());
				csvPrinterDagMore.flush();
			} catch (IOException e) {
				logger.error(e);
				e.printStackTrace();
			}

		}
		try {
			csvPrinterApp.close();
			csvPrinterDagDtl.close();
			csvPrinterDagMore.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void writeVertices(Vertices vertices_rewrite) {
		// Read hashmap and write to csv writer

		try {
			csvPrinterVertices.printRecord(vertices_rewrite.getVertexName(), vertices_rewrite.getVertexId(),
					vertices_rewrite.getDagId(), vertices_rewrite.getVertexSucceededTasks(),
					vertices_rewrite.getVertexFailedTasks(), vertices_rewrite.getVertexAvgtaskDuration(),
					vertices_rewrite.getVertexMintaskDuration(), vertices_rewrite.getVertexMaxtaskDuration(),
					vertices_rewrite.getVertexRunDuration(), vertices_rewrite.getVertexStartTs(),
					vertices_rewrite.getVertexEndTs(), vertices_rewrite.getVertexStatus());
			csvPrinterVertices.flush();
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}
	public void createVerticesFile() {
		File directory = new File(".");
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
		Date date = new Date();
		String dateAppend = dateFormat.format(date);
		try {
			verticesWriter = Files.newBufferedWriter(
					Paths.get(directory.getCanonicalPath() + "/VerticesMetric-" + dateAppend + ".csv"));
			csvPrinterVertices = new CSVPrinter(verticesWriter,
					CSVFormat.DEFAULT.withHeader("vertexName", "vertexId", "dagId", "vertexSucceededTasks",
							"vertexFailedTasks", "vertexAvgtaskDuration", "vertexMintaskDuration",
							"vertexMaxtaskDuration", "vertexRunDuration", "vertexStartTs", "vertexEndTs",
							"vertexStatus"));
		} catch (IOException e) {
			logger.error(e);
			e.printStackTrace();
		}
	}

}
