-- 06.03.2018

drop table hive_cc1;

 create table hive_cc1 (
    ctime  DOUBLE,
    carnum STRING
   ) 
     STORED AS Parquet
     LOCATION 'hdfs://10.242.5.88:9000/user/data/cc503.parquet';
     
select * from hive_cc1

select count(*) from hive_cc1

select ctime,cast(ctime as timestamp),carnum from hive_cc1

select max(ctime) from hive_cc1

select count(*) from hive_cc1

/*
 without index :  
1) 1 min 8 sec
2) 1 min 8 sec
3) 46301ms
-- WITH INDEX:
1)  57352ms
2)
*/
select ds.cnt,
       qe.*
  from hive_cc1 qe,
       (
       select ctime,
              cast(ctime as timestamp),
              count(carnum) as cnt 
         from hive_cc1 q
        group by ctime,cast(ctime as timestamp)
        order by cnt desc
        limit 1
       ) ds
 where qe.ctime = ds.ctime


CREATE INDEX idx_hive_cc1_ctime
ON TABLE hive_cc1 (ctime)
AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler'
WITH DEFERRED REBUILD
IN TABLE idx_hive_cc1_ctime_table
COMMENT 'index comment';

ALTER INDEX idx_hive_cc1_ctime ON hive_cc1 REBUILD; -- populationg index 

-- with    index : 
-- 12196 ms
-- 8006  ms
-- 11622 ms
------------
-- without 
--
select count(*) from hive_cc1 where ctime=1520318910

drop index idx_hive_cc1_ctime on hive_cc1;

--=================================================