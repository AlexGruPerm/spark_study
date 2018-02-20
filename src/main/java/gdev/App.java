package gdev;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
 

public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {
		logger.info("info message");

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
		HadoopSimple hs = new HadoopSimple();
		try {
			hs.write_into_hdfs();
		} catch (IOException e) {
			logger.debug(e.fillInStackTrace());
		}
		
		*/
		
		WriteParquetMR wp = new WriteParquetMR();
		
		try {
			wp.write_simple();
		} catch (Exception e) {
			logger.debug(e.fillInStackTrace());
		}
		
		
 
		}

	/*
	 * public void debug_ds(
            String apploaded_prq_fpath
           ){
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL basic example")
				.config("spark.master", "local[*]")
				.config("spark.sql.crossJoin.enabled", "true").getOrCreate();

		Dataset<Row> sqlLoadedDF = spark.read().load(apploaded_prq_fpath);

		sqlLoadedDF.createOrReplaceTempView("v_aload");

		Dataset<Row> usrs_CntApplod_WeekReg = spark
				.sql("SELECT v_aload.time,  current_timestamp as cts, datediff(current_timestamp,v_aload.time) as dd, date_add(v_aload.time,7) as t7 FROM v_aload ");

		usrs_CntApplod_WeekReg.show();     
}
	
	 * */

}
