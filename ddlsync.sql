/*
The purspose of this script is to create ddl files to sync the siebel schema. The main focus is on tables, columns, indexes. 
It is specifically designed for SIEBEL Team and should not be used for any other purpose without modification/review of the code.
===========
Requirement
===========
1) It creates a database link to target server. So a proper network level connection is required.

===========
Author
===========
Anjul Sahu	
anjulsahu@gmail.com

===========
History
===========
22/10/2010	1.0	Initial Version
04/11/2010	1.1 Remove some bugs
11/11/2010	1.2 Fixed pre_create_ddl to remove DESC clause in index
				Introducing index renaming when column is same. 
*/


SET LONG 500000
SET LINE 200
SET PAGES 0
SET FEEDBACK OFF VERIFY OFF TIMING OFF

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM	1. Read source and target Siebel schema structure (tables and indexes)
REM  ---------------------------------------------------------------------------------------------------------------------------	

SELECT 'Start time: '||TO_CHAR (SYSDATE, 'mm/dd/yyyy hh24:mi:ss')
FROM   SYS.dual
;

PROMPT	Creating database link to Target Database Schema
REM  ---------------------------------------------------------------------------------------------------------------------------	
REM	create a db link to target
REM  ---------------------------------------------------------------------------------------------------------------------------	
--CREATE DATABASE LINK REMOTE_SERVER connect to SIEBEL identified by <password> using '';

-- to avoid ORA-02085
alter session set global_names=false; 

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM	2. Either have a pre-defined set of tables that are large, or dynamically generate this list based on size in target schema - Set T0
REM  ---------------------------------------------------------------------------------------------------------------------------	
 
PROMPT dropping temporary set T0  

drop table xxgtt_sync_t0
;

PROMPT creating temporary set T0
PROMPT xxgtt_sync_t0 - List of very large tables

create global temporary table xxgtt_sync_t0 
on commit preserve rows 
as 
select * from (
select segment_name "table_name" 
from user_segments@remote_server 
where segment_type='TABLE' and segment_name not in ('S_DOCK_TXN_LOG','S_WFA_STPRP_LOG','S_WFA_INSTP_LOG','CX_ERROR_LOG')
group by segment_name 
order by 1 desc) 
where rownum<31
;
REM  ---------------------------------------------------------------------------------------------------------------------------	
REM	dynamic list - gives irrelavant tables like CR2752_S_ORDER_ITEM, LOG tables - S_WFA_STPRP_LOG etc
REM	Pre-Defined - YES (Siebel team to provide list of tables)
REM  ---------------------------------------------------------------------------------------------------------------------------	

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM	3. Compare and identify the differences between the two
REM    a. All tables that are present in source but not in target - Set T1
REM  ---------------------------------------------------------------------------------------------------------------------------	
	
PROMPT dropping temporary set T1
PROMPT dropping table xxgtt_sync_t1	

drop table xxgtt_sync_t1
;
	
PROMPT creating table xxgtt_sync_t1
PROMPT xxgtt_sync_t1 is list of tables which are not in target but present in source

create global temporary table xxgtt_sync_t1 
on commit preserve rows
as 
select table_name from user_tables where table_name not like 'XXGTT_SYNC%'
minus
select table_name from user_tables@remote_server where table_name not like 'XXGTT_SYNC%';
	
REM  ---------------------------------------------------------------------------------------------------------------------------		
REM    b. All tables in source that have additional columns (without default values) than the table in target - set T2
REM  ---------------------------------------------------------------------------------------------------------------------------	

PROMPT Creating temporary function xxfn_sync_getdd

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM this is to compare long datatype column data_default
REM  ---------------------------------------------------------------------------------------------------------------------------	

create or replace function xxfn_sync_getdd(l_tname in varchar2, l_colname in varchar2)
return varchar2
is
l_data_default user_tab_columns.data_default%type;
begin
	select data_default into l_data_default from user_tab_columns
	where table_name=l_tname and column_name=l_colname;
	return l_data_default;
end;
/

PROMPT Dropping xxgtt_sync_t2
drop table xxgtt_sync_t2
;

PROMPT Creating set T2 - list of tables in source which have additional column without default values than the table in target
create global temporary table xxgtt_sync_t2
on commit preserve rows
as
select table_name, column_name
from user_tab_columns
where xxfn_sync_getdd(table_name,column_name) is null 
and table_name not like 'XXGTT_SYNC%'
minus
select table_name, column_name
from user_tab_columns@remote_server
where xxfn_sync_getdd(table_name,column_name) is null 
and  table_name not like 'XXGTT_SYNC%'
;	

REM  ------------------------------------------------------------------------------------------------------------------------
REM    c. All tables in source that have additional columns (with default values) than the table in target - set T3
REM  ------------------------------------------------------------------------------------------------------------------------
PROMPT dropping xxgtt_sync_t3

drop table xxgtt_sync_t3;

PROMPT Creating set T3 - list of tables in source which have additional column with default values than the table in target

create global temporary table xxgtt_sync_t3
on commit preserve rows
as
select table_name,column_name
from user_tab_columns 
where xxfn_sync_getdd(table_name,column_name) is not null
and table_name not like 'XXGTT_SYNC%'
minus
select table_name,column_name
from user_tab_columns@remote_server 
where xxfn_sync_getdd(table_name,column_name) is not null
and table_name not like 'XXGTT_SYNC%';

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM		d. All tables in source that have fewer columns that the table in target - set T4
REM  ---------------------------------------------------------------------------------------------------------------------------	

drop table xxgtt_sync_t4;

PROMPT Creating set T4 - All tables in source that have fewer columns that the table in target

create global temporary table xxgtt_sync_t4
on commit preserve rows
as
select table_name, column_name
from user_tab_columns@remote_server
where table_name in (select table_name from user_tables where table_name not like 'XXGTT_SYNC%')
minus
select table_name, column_name
from user_tab_columns where table_name not like 'XXGTT_SYNC%'
;

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM		e. All indexes in source that are missing in target - set X1
REM  ---------------------------------------------------------------------------------------------------------------------------	

drop table xxgtt_sync_x1
;
PROMPT Creating set X1 - All indexes in source that are missing in target 
create global temporary table xxgtt_sync_x1
on commit preserve rows
as
select index_name,table_name from user_indexes
minus
select index_name,table_name from user_indexes@remote_server
;

REM  ---------------------------------------------------------------------------------------------------------------------------------	
REM		f. All indexes in source that have different indexed columns compared to target - set X2
REM  ---------------------------------------------------------------------------------------------------------------------------------	

drop table xxgtt_sync_x2
;
PROMPT Creating set X2 - All indexes in source that have different indexed columns compared to target 
create global temporary table xxgtt_sync_x2
on commit preserve rows
as
SELECT   A.index_name, A.table_name, 
         B.column_name, B.column_position
FROM     user_indexes A, user_ind_columns B
WHERE    A.index_name IN
         (
         SELECT index_name
         FROM   user_indexes@remote_server
         )
AND      B.index_name = A.index_name
AND      B.table_name = A.table_name
MINUS
SELECT   A.index_name, A.table_name, 
         B.column_name, B.column_position
FROM     user_indexes@remote_server A, user_ind_columns@remote_server B
WHERE    B.index_name = A.index_name
AND      B.table_name = A.table_name
;

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM		g. All indexes in target that are DESC order indexes - set X3
REM  ---------------------------------------------------------------------------------------------------------------------------	

drop table xxgtt_sync_x3
;
PROMPT Creating set X3 - all indexes in target that are in descending order
create global temporary table xxgtt_sync_x3
on commit preserve rows
as
select  table_name,index_name,column_name
from user_ind_columns@remote_server
where DESCEND='DESC'
;



REM  ---------------------------------------------------------------------------------------------------------------------------	
REM		4. Generate pre-ddl sync drop script as follows : 
REM			a. Include all indexes in set X3
REM  ---------------------------------------------------------------------------------------------------------------------------	

PROMPT creating SYNC scripts
spool pre_ddl_drop.sql
select 'spool pre_ddl_drop.log' from dual
;
select 'drop index '||index_name||';' from xxgtt_sync_x3
;

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM			b. Include all indexes in set X2 that are on tables belonging to set T0
REM  ---------------------------------------------------------------------------------------------------------------------------	
 
select distinct 'drop index '|| index_name||';' from xxgtt_sync_x2 
where table_name in (select table_name from xxgtt_sync_t0)
;
select 'spool off' from dual;
spool off

REM  ---------------------------------------------------------------------------------------------------------------------------	
REM 5. Generate pre-ddl sync create script as follows : 
REM    a. Include all indexes in set X3. Create them as default ASC indexes.
REM  ---------------------------------------------------------------------------------------------------------------------------	


spool pre_ddl_create.sql	
select 'spool pre_ddl_create.log' from dual;

execute dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'STORAGE', FALSE);


execute DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',FALSE);

PROMPT   

select replace(to_char(dbms_metadata.get_ddl('INDEX',index_name)), ' DESC') ||' nologging parallel 10;'
from xxgtt_sync_x3
;

PROMPT  

REM  ----------------------------------------------------------------------------------	
REM    b. Include all indexes in sets X1 and X2 that are on tables belonging to set T0
REM  ----------------------------------------------------------------------------------


select replace(to_char(dbms_metadata.get_ddl('INDEX',index_name)), ' DESC') ||' nologging parallel 10;' 
from xxgtt_sync_x1  
where table_name in (select table_name from xxgtt_sync_t0);

PROMPT  

select replace(to_char(dbms_metadata.get_ddl('INDEX',index_name)), ' DESC')||' nologging parallel 10;'  
from xxgtt_sync_x2 where table_name in (select table_name from xxgtt_sync_t0);


select 'alter index '||index_name||' logging noparallel;' from xxgtt_sync_x1
UNION
select 'alter index '||index_name||' logging noparallel;' from xxgtt_sync_x2
UNION
select 'alter index '||index_name||' logging noparallel;' from xxgtt_sync_x3
;

--Index rename snippet 

PROMPT Generating rename script for INDEXES with different name but on same column

--create staging table to store the index details
CREATE TABLE xxtemptable (source_or_target varchar2(10), table_name varchar2(255), index_name varchar2(255), column_list varchar2(4000));

--create the proc that will compare the index in source and target to identify the ones that are the same except for the index name
CREATE OR REPLACE PROCEDURE xxTESTPROC
AS
szindexcols varchar2(4000) := '';
szprevtable varchar2(255) := '';
szprevindex varchar2(255) := '';
sztable varchar2(255) := '';
szindex varchar2(255) := '';
nrow integer := 1;
BEGIN
FOR idxrecord IN (SELECT A.table_name, A.index_name, B.column_name, B.column_position from user_indexes A, user_ind_columns B where B.index_name = A.index_name and B.table_name = A.table_name order by A.table_name, A.index_name, B.column_position) 
LOOP 
    IF nrow = 1 THEN 
        szprevtable := idxrecord.table_name;
        szprevindex := idxrecord.index_name;
        nrow := 2;
    ELSE
        sztable := idxrecord.table_name;
        szindex := idxrecord.index_name;
        IF sztable = szprevtable AND szindex = szprevindex Then
            szindexcols := szindexcols || ',' || idxrecord.column_name;
        ELSE
            INSERT INTO xxtemptable SELECT 'SOURCE', szprevtable, szprevindex, szindexcols from dual;
            szprevtable := sztable;
            szprevindex := szindex;
            szindexcols := idxrecord.column_name;
        END IF;
    END IF; 

END LOOP;

szindexcols := '';
szprevtable := '';
szprevindex := '';
sztable := '';
szindex := '';
nrow := 1;

FOR idxrecord IN (SELECT A.table_name, A.index_name, B.column_name, B.column_position from user_indexes@remote_server A, user_ind_columns@remote_server B where B.index_name = A.index_name and B.table_name = A.table_name order by A.table_name, A.index_name, B.column_position) 
LOOP 

    IF nrow = 1 THEN 
        szprevtable := idxrecord.table_name;
        szprevindex := idxrecord.index_name;
        nrow := 2;
    ELSE
        sztable := idxrecord.table_name;
        szindex := idxrecord.index_name;
        IF sztable = szprevtable AND szindex = szprevindex Then
            szindexcols := szindexcols || ',' || idxrecord.column_name;
        ELSE
            INSERT INTO xxtemptable SELECT 'TARGET', szprevtable, szprevindex, szindexcols from dual;
            szprevtable := sztable;
            szprevindex := szindex;
            szindexcols := idxrecord.column_name;
        END IF;
    END IF; 

END LOOP;

END xxTESTPROC;
/

--truncate the staging table before running the procedure
truncate table xxtemptable;

--execute the procedure to generate the index column details in the staging table
execute xxTESTPROC;

--spool the output of the below to generate the scripts for renaming the index names for all indexes that are the same in source and target except for the index name
select 'ALTER INDEX ' || B.index_name || ' RENAME TO ' || A.index_name || ';'
from xxtemptable A, xxtemptable B
where A.source_or_target = 'SOURCE' and B.source_or_target = 'TARGET' and 
A.table_name = B.table_name and 
A.column_list = B.column_list and 
A.index_name <> B.index_name;



REM  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	
REM		c. Include column addition statements for tables belonging to set T3 that are also part of set T0. 
REM		This is to ensure that for the large tables where we are adding columns with default values, we add the additional columns
REM		then use parallelism to update the value to the default value and then set the default value on the columns.
REM  ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------	

-- data precision is not included
select 'alter table '||table_name||' add '||column_name||' '|| data_type || ' ' || decode(data_length,0,null,'('||data_length||')') || ' ' || decode(nullable,'N','NOT NULL') ||';' from 
user_tab_columns 
where table_name in (select table_name from xxgtt_sync_t3 where table_name in (select table_name from xxgtt_sync_t0)) 
AND column_name in (select column_name from xxgtt_sync_t3)
;
select 'update /*+ parallel(a,10) */  '||table_name||' a set '||column_name||'='||xxfn_sync_getdd(table_name,column_name)||';' from xxgtt_sync_t3;
select 'commit;' from dual;
select 'alter table '||table_name||' modify '||column_name||' default '||xxfn_sync_getdd(table_name,column_name)||';' from xxgtt_sync_t3;
select 'spool off' from dual;

REM  ---------------------------------------------------------------------------------------------------------------------------	 
REM		6. Generate post-ddl sync drop script as follows : 
REM		    a. Include all indexes in set X3
REM  ---------------------------------------------------------------------------------------------------------------------------		

spool post_ddl_sync_drop.sql
select 'spool post_ddl_sync_drop.log' from dual;
select 'drop index '||index_name||';' from xxgtt_sync_x3;
select 'spool off' from dual;
spool off

REM  ---------------------------------------------------------------------------------------------------------------------------	 
REM		7. Generate post-ddl sync create script as follows : 
REM		    a. Include all indexes in set X3 (with DESC order clause)
REM  ---------------------------------------------------------------------------------------------------------------------------	

spool post_ddl_sync_create.sql
select 'spool post_ddl_sync_create.log' from dual;
execute DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM,'SQLTERMINATOR',FALSE);

select to_char(dbms_metadata.get_ddl('INDEX',index_name))||' nologging parallel 10;' from xxgtt_sync_x3;
select 'alter index '||index_name||' logging noparallel;' from xxgtt_sync_x3;
select 'spool off' from dual;
spool off 
 
REM  ---------------------------------------------------------------------------------------------------------------------------	
REM		8. Generate stats collection script as follows : 
REM		    a. Include all tables in sets T1, T2, T3 and T4 and all indexes on these tables.
REM			b. Include all indexes in sets X1, X2 and X3
REM  ---------------------------------------------------------------------------------------------------------------------------	

spool get_stats.sql
select 'spool get_stats.log' from dual;
select distinct script from 
(select 'exec dbms_stats.gather_table_stats(ownname=>''SIEBEL'',tabname=>'''||table_name||''',estimate_percent=>10, degree=>10, CASCADE=>TRUE);'  script from xxgtt_sync_t1
UNION
select 'exec dbms_stats.gather_table_stats(ownname=>''SIEBEL'',tabname=>'''||table_name||''',estimate_percent=>10, degree=>10, CASCADE=>TRUE);'  script from xxgtt_sync_t2
UNION
select 'exec dbms_stats.gather_table_stats(ownname=>''SIEBEL'',tabname=>'''||table_name||''',estimate_percent=>10, degree=>10, CASCADE=>TRUE);'  script from xxgtt_sync_t3
UNION
select 'exec dbms_stats.gather_table_stats(ownname=>''SIEBEL'',tabname=>'''||table_name||''',estimate_percent=>10, degree=>10, CASCADE=>TRUE);'  script from xxgtt_sync_t4
UNION
select 'exec dbms_stats.gather_index_stats(ownname=>''SIEBEL'',indname=>'''||index_name||''',estimate_percent=>10, degree=>10);'  script from xxgtt_sync_x1
UNION
select 'exec dbms_stats.gather_index_stats(ownname=>''SIEBEL'',indname=>'''||index_name||''',estimate_percent=>10, degree=>10);'  script from xxgtt_sync_x2
UNION
select 'exec dbms_stats.gather_index_stats(ownname=>''SIEBEL'',indname=>'''||index_name||''',estimate_percent=>10, degree=>10);'  script from xxgtt_sync_x3
); 

select 'spool off' from dual;
spool off

drop function xxfn_sync_getdd;

SELECT 'End time: '||TO_CHAR (SYSDATE, 'mm/dd/yyyy hh24:mi:ss')
FROM   SYS.dual
;