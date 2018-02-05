package gdev;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {

		logger.info("info message");
		logger.debug("debug message");
		
	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL xyz 333")
	    		    .config("spark.master", "spark://192.168.1.8:7077")
	    		    .getOrCreate();

	       spark.close();
	}

}
