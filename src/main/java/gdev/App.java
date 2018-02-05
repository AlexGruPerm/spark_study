package gdev;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {

		logger.info("hello info");
		logger.debug("hello debug");
		
	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL basic example")
	    		    .config("spark.master", "spark://192.168.1.8:7077")  //spark://127.0.0.1:7077")
	    		    .getOrCreate();

	       spark.close();
	       
	       /*
	    	Dataset<Row> df_appl = spark.read().load(loaded_prq_fpath);
	    	df_appl.show();
	    	df_appl.printSchema();
	    	
	    	Dataset<Row> df_reg = spark.read().load(regstr_prq_fpath);
	    	df_reg.show();
	    	df_reg.printSchema();	
	    	*/

	}

}
