package gdev;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class PostgresIntoParquet { 
	
	final static Logger logger = Logger.getLogger(PostgresIntoParquet.class);
	
	void load_table_into_pqrquet(String       p_table_name,
			                     String       p_path_to_parquet_dir,
			                     SparkSession spark){
		
	       Dataset<Row> jdbcDF = spark.read()
	    		   .format("jdbc")
	    		   .option("url","jdbc:postgresql://10.242.5.62:5432/db_ris_mkrpk" )
	    		   .option("dbtable", "prm_salary." + p_table_name)
	    		   .option("user", "prm_salary")
	    		   .option("password", "prm_salary")
	    		   .option("driver", "org.postgresql.Driver")
	    		   .load();

	       jdbcDF.printSchema();
	       jdbcDF.show();   
	       jdbcDF.write().mode(SaveMode.Overwrite).parquet(p_path_to_parquet_dir+'\\'+p_table_name+".parquet"); 
	       
	       Dataset<Row> ds = spark.read().load(p_path_to_parquet_dir+'\\'+p_table_name+".parquet");
    	   ds.show();
    	   ds.printSchema();
    	    
	}
	
	void execute_sql_query(SparkSession spark){

 	    Dataset<Row> sql_sr = spark.read().load("C:\\spark_data\\sl_report.parquet");
 	    sql_sr.createOrReplaceTempView("sr");
 	    
 	    Dataset<Row> sql_srd = spark.read().load("C:\\spark_data\\sl_report_data.parquet");
 	    sql_srd.createOrReplaceTempView("srd");

		long t1 = System.nanoTime();
 	    
	    Dataset<Row> sql_res = spark.sql("select sl_report_status_type_id, "
	    		                            + "  sum(srd.val) as vsum "
	    		                            + " from  sr,  "
	    		                            + "       srd "
	    		                            + " where sr.sl_report_id = srd.sl_report_id "
	    		                            + " group by sl_report_status_type_id");

		sql_res.show();
		
	    long t2 = System.nanoTime();
	    long elapsedTimeInSeconds = (t2 - t1) / 1000000000;
	    
	    //sql_res.describe().show();
		
	    logger.info(">>> durstion : "+String.valueOf(elapsedTimeInSeconds)+" seconds.");
	}
	
	void show_spark_meta(SparkSession spark){
		spark.catalog().listDatabases().show();
		spark.catalog().listTables();
	}

}
