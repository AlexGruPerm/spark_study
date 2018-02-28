package gdev;

import java.io.IOException;

import org.apache.log4j.Logger;
 
public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {
		logger.info("Begin main[] .......");

		//SparkPostgre sp = new SparkPostgre();
		 
		//win
		/*
		String spark_url = "spark://192.168.1.5:7077";
		String data_path = "C:\\spark_data";
		*/
		//lnx
		
		//String spark_url = "spark://192.168.1.8:7077";
		//String data_path = "/root/data";

		//sp.run(spark_url,data_path);
		 
		/*
		 * 111
		HadoopSimple hs = new HadoopSimple();
		try {
			hs.write_into_hdfs();
		} catch (IOException e) {
			logger.debug(e.fillInStackTrace());
		}
		*/

		/* 222
		WriteParquetMR wp = new WriteParquetMR();
		try {
			wp.write_simple();
		} catch (Exception e) {
			logger.warn(e.fillInStackTrace());
		}
		*/
		
		MultipleMR mmr = new MultipleMR();
		try {
			mmr.run(args);
			// INFO  org.apache.hadoop.conf.Configuration.deprecation  - session.id is deprecated. Instead, use dfs.metrics.session-id
		} catch (Exception e) {
			logger.warn(e.fillInStackTrace());
		}

		logger.info("End main[] .......");
		}



}
