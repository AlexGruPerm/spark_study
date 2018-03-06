-- 06.03.2018

drop table hive_cc1;

 create table hive_cc1 (
    ctime  DOUBLE,
    carnum STRING
   ) 
     STORED AS Parquet
     LOCATION 'hdfs://10.242.5.88:9000/user/data/cc503.parquet';
     
select * from hive_cc1

select ctime,cast(ctime as timestamp),carnum from hive_cc1

/*
insert into hive_cc1 select 1520323416,'0'
select ctime,cast(ctime/1000 as timestamp),carnum from hive_cc1
*/

/*
Correct java code

		  	    MessageType CAM_CAR_SNAP_FILE_SCHEMA = Types.buildMessage()
			  		      .optional(INT64).as(TIMESTAMP_MILLIS).named("ctime")
			  		      .optional(BINARY).as(UTF8).named("carnum")
			  		      .named("cam_car");
...
  Long start_ts = (long) 1520318907;
...
group_cc.add("ctime",   start_ts); // !!! *1000 !!!

*/

Java insert into 
	    String hdfsuri = "hdfs://10.242.5.88:9000"; // "hdfs://192.168.1.14:8020";
		String path = "/user/data/";
	    String fileName = "cc503.parquet/part1";
	    
Hive can read it but with empty ctime

--#######################################################################
