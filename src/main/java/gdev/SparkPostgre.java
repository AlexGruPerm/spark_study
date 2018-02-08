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
		
		String tbl_name = "sl_report_status";
		/*
		sl_report
		sl_report_data
		sl_report_status
		*/
		
	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL") 
	    		    .config(conf)
	    		    .getOrCreate();	
	       
	       
	       Dataset<Row> jdbcDF = spark.read()
	    		   .format("jdbc")
	    		   .option("url","jdbc:postgresql://10.242.5.62:5432/db_ris_mkrpk" )
	    		   .option("dbtable", "prm_salary."+tbl_name)
	    		   .option("user", "prm_salary")
	    		   .option("password", "prm_salary")
	    		   .option("driver", "org.postgresql.Driver")
	    		   .load();

	       jdbcDF.printSchema();
	       jdbcDF.show();   

	       String parquetPath = "C:\\spark_data\\"+tbl_name+".parquet";	 
	       
	       jdbcDF.write().mode(SaveMode.Overwrite).parquet(parquetPath); 
//	       jdbcDF.select("code").write().mode(SaveMode.Overwrite).parquet(parquetPath);

	       Dataset<Row> ds = spark.read().load(parquetPath);
    	   ds.show();
    	   ds.printSchema();
    	   
	       spark.close();

	}

}
