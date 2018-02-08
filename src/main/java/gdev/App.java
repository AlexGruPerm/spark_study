package gdev;

/*
import static org.apache.parquet.schema.OriginalType.TIMESTAMP_MILLIS;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
*/

//import java.io.File;
//import java.io.IOException;

//import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;
 
import org.apache.parquet.Preconditions;
/*
import org.apache.parquet.Preconditions;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
*/ 
import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
//import org.apache.hadoop.fs.Path;
//import org.junit.rules.TemporaryFolder;

//import com.fasterxml.jackson.core.JsonToken;
//import com.fasterxml.jackson.databind.JsonNode;

public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {
		logger.info("info message");
		logger.debug("debug message");
		
		//String jsonPath = "/root/data/people.json";
		String jsonPath = "C:\\spark_data\\people3.json";
		
		//String parquetPath = "/root/data/names.parquet";
		String parquetPath = "C:\\spark_data\\names.parquet";
		
		//String sparkMaster = "spark://192.168.1.8:7077"; 
		
		//String sparkMaster = "spark://192.168.1.5:7077";
		//"spark://169.254.16.5:7077"
		
		
		String sparkMaster = "spark://169.254.16.5:7077";
		//String sparkMaster = "local[*]";
		  
		//=======================================================
		
		//+File.separator+
		
		SparkConf conf = new SparkConf();
		conf.set("spark.master",sparkMaster);
		conf.set("spark.sql.crossJoin.enabled", "true");

		/*
		File fl_prq = new File(parquetPath);	   
	  	    if (fl_prq.exists()==true) {
	  	      logger.info("Recreate parquet file for app_loaded events: "+fl_prq.getAbsolutePath());
		      Preconditions.checkArgument(fl_prq.delete(), "Could not remove parquet file "+fl_prq.getAbsolutePath());
	  	    } else {
	  	      logger.info("SUCCESS File does not exists : "+fl_prq.getAbsolutePath());
	  	    }
	  	    */
	  	//Path pth_file_prq = new Path(fl_prq.toString());
	  	  
	  	    

	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL") 
	    		    .config(conf)
	    		    .getOrCreate();

	       logger.debug("CONF:"+spark.conf().getAll().toString()); 

	    FileWriter f0;
		try {
			f0 = new FileWriter("C:\\spark_data\\people3.json");
		       for(int i=0; i < 1000000; i++)
		       {
				f0.write("{\"name\":\""+RandomStringUtils.randomAlphanumeric(10).toUpperCase()+"\"}");
		       }
		       f0.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      // String newLine = System.getProperty("line.separator");

	       
	       Dataset<Row> df = spark.read().option("header","false").json(jsonPath);
	       df.printSchema();
	       df.show();
	       df.select("name").write().mode(SaveMode.Overwrite).parquet(parquetPath);
        

	       Dataset<Row> ds = spark.read().load(parquetPath);
	       
    	   ds.show();
    	   ds.printSchema();

    	   Dataset<Row> sql_ds = spark.read().load(parquetPath);
    	   sql_ds.createOrReplaceTempView("vusr");

   		   Dataset<Row> sql_ds_res = spark.sql("SELECT name, count(*) as grp_by_key FROM vusr group by name order by 1 desc");
   		   sql_ds_res.show(); 


   	
   		   /*
   		   Dataset<Row> sql_ds_cnt = spark.sql("SELECT count(*) as cnt_rows_parquet FROM vusr");
   		   sql_ds_cnt.show();   
	       */
	      
	       /*
		try{
	       MessageType DS_FILE_SCHEMA = Types.buildMessage()
		  		      .required(BINARY).as(UTF8).named("name")
		  		      .named("dataset");
	       
	       //example: https://www.programcreek.com/java-api-examples/index.php?api=parquet.hadoop.ParquetWriter
	        SimpleGroupFactory GROUP_FACTORY_DS = new SimpleGroupFactory(DS_FILE_SCHEMA);

	  	    File fl_ds = new File(prq_full_filename);
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
/*
	 	Dataset<Row> df_appl = spark.read().load(prq_full_filename);
    	df_appl.show();
    	df_appl.printSchema();
		*/
    	
		        spark.close();
		}


}
