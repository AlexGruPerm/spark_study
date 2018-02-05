package gdev;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class App {

	final static Logger logger = Logger.getLogger(App.class);
	
	public static void main(String[] args) {
		String warehouseLocation = "file:"+"/root/spark-warehouse";
		logger.info("info message");
		logger.debug("debug message");
		logger.info("Sys user.dir : "+ System.getProperty("user.dir"));
		
	       SparkSession spark = SparkSession
	    	    	.builder()
	    		    .appName("Java Spark SQL xyz 333")
	    		    .config("spark.master", "spark://192.168.1.8:7077")
	    		    .config("spark.sql.warehouse.dir", warehouseLocation) //Path does not exist: file:/C:/spark_study/employees.json;
	    		    //.config("spark.sql.warehouse.dir", "hdfs://192.168.1.8:50070/user/hive/warehouse/")
	    		    //.enableHiveSupport()
	    		    //.config(CATALOG_IMPLEMENTATION.key, "hive")
	    		    .getOrCreate();
	       
/*
 SparkSession sparkSession = SparkSession
            .builder().appName(JsonToHive.class.getName())
            //.config("spark.sql.warehouse.dir", "hdfs://localhost:50070/user/hive/warehouse/")
            .enableHiveSupport().master(master).getOrCreate();
*/
	       
	       SQLContext sqlCtx = spark.sqlContext();
	       Dataset<Row> rowDataset = sqlCtx.read().json("employees.json");
	       rowDataset.printSchema();
	       rowDataset.createOrReplaceGlobalTempView("employeesData");
	       
	       //JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
	       //logger.info(">>>>>>>>> jsc.getSparkHome="+jsc.getSparkHome().toString()); // jsc.getSparkHome=Optional[C:\spark-2.2.1-bin-hadoop2.7]

	       spark.close();
	}

}
