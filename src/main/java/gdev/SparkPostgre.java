package gdev;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class SparkPostgre {
	
	final static Logger logger = Logger.getLogger(SparkPostgre.class);

	public static void main(String[] args) {
	
		String sparkMaster = "spark://169.254.16.5:7077";//"local[*]"
		
		SparkConf conf = new SparkConf();
		conf.set("spark.master",sparkMaster);
		conf.set("spark.sql.crossJoin.enabled", "true");
        //spark.sql.inMemoryColumnarStorage.compressed	true
		
		String p_path_to_parquet_dir = "C:\\spark_data";
		String tbl_name              = "sl_report_p4_data";
		
		PostgresIntoParquet pp = new PostgresIntoParquet();
		
	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL") 
	    		    .config(conf)
	    		    .enableHiveSupport()
	    		    .getOrCreate();	
		
		//pp.load_table_into_pqrquet(tbl_name, p_path_to_parquet_dir, spark);
	    pp.execute_sql_query(spark);
	    //pp.show_spark_meta(spark);

//	не таблица, а одно полей. jdbcDF.select("code").write().mode(SaveMode.Overwrite).parquet(parquetPath);

	     spark.close();

	}

}
