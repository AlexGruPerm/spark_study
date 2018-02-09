package gdev;
 
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
/*
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
*/
import org.apache.spark.sql.SparkSession;

public class SparkPostgre {
	
	final static Logger logger = Logger.getLogger(SparkPostgre.class);

	void run(String spark_url,String data_path) {
	
		String sparkMaster = spark_url;

		SparkConf conf = new SparkConf();
		conf.set("spark.master",sparkMaster);
		conf.set("spark.sql.crossJoin.enabled", "true");
        //spark.sql.inMemoryColumnarStorage.compressed	true
		
		//String p_path_to_parquet_dir = data_path;
		//String tbl_name              = "sl_report_p4_data";
		
		PostgresIntoParquet pp = new PostgresIntoParquet(data_path);

	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL") 
	    		    .config(conf)
	    		    .getOrCreate();	
		
		//pp.load_table_into_pqrquet("sl_report_data", data_path, spark);
	    //pp.execute_sql_query(spark);
	       pp.execute_sql_query2(spark);
	    //pp.show_spark_meta(spark);

//jdbcDF.select("code").write().mode(SaveMode.Overwrite).parquet(parquetPath);

	     spark.close();

	}

}
