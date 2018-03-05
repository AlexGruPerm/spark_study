-- 05.03.2018

drop table hive_cc1;

 create table hive_cc1 (
    ctime  DOUBLE,
    carnum STRING
   ) 
     STORED AS Parquet
     LOCATION 'hdfs://10.242.5.88:9000/user/data/cc503.parquet';
     
select * from hive_cc1

insert into hive_cc1 select unix_timestamp(),'r345mr'
select ctime,cast(ctime as timestamp),carnum from hive_cc1


select unix_timestamp_ms()



ctime               |carnum |
--------------------|-------|
2018-03-05 18:51:37 |r345mr |

$ hadoop jar  parquet-tools-1.9.0.jar schema hdfs://10.242.5.88:9000/user/data/cc503.parquet

message hive_schema {
  optional double ctime;
  optional binary carnum (UTF8);
}

Java insert into 
	    String hdfsuri = "hdfs://10.242.5.88:9000"; // "hdfs://192.168.1.14:8020";
		String path = "/user/data/";
	    String fileName = "cc503.parquet/part1";
	    
Hive can read it but with empty ctime

--#######################################################################
