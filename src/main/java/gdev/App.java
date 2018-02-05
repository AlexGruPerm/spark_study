package gdev;

import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;

public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {
		logger.info("info message");
		logger.debug("debug message");
		
	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL") 
	    		    .config("spark.master", "spark://192.168.1.8:7077")
	    		    .getOrCreate();
	       
	       logger.debug("CONF:"+spark.conf().getAll().toString()); 
		/*
		 * 2018-02-06 02:03:27 4188 [main] DEBUG gdev.App -
		 * CONF:Map(spark.driver.host -> 192.168.1.5, 
		 * spark.driver.port ->
		 * 59749, spark.app.name -> Java Spark SQL, 
		 * spark.executor.id -> driver,
		 * spark.master -> spark://192.168.1.8:7077, 
		 * spark.app.id ->
		 * app-20180205160323-0017)
		 */
	       
	       
	       Dataset<Row> usersDF = spark.read().load("/root/data/users.parquet"); 
	       
	        
		/*
		try{
	       MessageType DS_FILE_SCHEMA = Types.buildMessage()
		  		      .required(BINARY).as(UTF8).named("name")
		  		      .named("dataset");
	       //example: https://www.programcreek.com/java-api-examples/index.php?api=parquet.hadoop.ParquetWriter
	        SimpleGroupFactory GROUP_FACTORY_DS = new SimpleGroupFactory(DS_FILE_SCHEMA);
 
	  	    File fl_ds = new File("/root/data");
	  	    logger.info("Recreate parquet file for app_loaded events: "+fl_ds.getAbsolutePath());
	  	    if (fl_ds.exists()==true) {
		      Preconditions.checkArgument(fl_ds.delete(), "Could not remove parquet file "+fl_ds.getAbsolutePath());
	  	    }
		    Path pth_ds = new Path(fl_ds.toString());
	       
		    ParquetWriter<Group> writer = ExampleParquetWriter.builder(pth_ds)
			        .withType(DS_FILE_SCHEMA)
			        .build();
		    
		    for (int j = 0;  j < 100; ++j) {
	        	    	Group group1 = GROUP_FACTORY_DS.newGroup(); 
	        		    group1.add("name",      "name_" + j);
	        		    writer.write(group1);
	        		    //.append("timestamp", System.currentTimeMillis());
	        		    writer.write(group1);
	        }
		    writer.close();

		}
		     catch (IOException e) {
			  logger.error(e.fillInStackTrace());
			 }
		*/
		        spark.close();
		}


}
