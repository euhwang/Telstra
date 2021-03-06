******************************************************************************;
* Copyright (c) 2015 by Eugene Hwang, Marissa Wiener, Scott Senkier          *;
*                                                                            *;
* Telstra Kaggle Competition			                                     *;
******************************************************************************;

**Define variables; 
/*%let file_locn = \\vmware-host\Shared Folders\Documents\GWU\DNSC 6279\Telstra\;*/
%let file_locn = C:/Users/Marissa/Documents/_GW MSBA/Data Mining/Kaggle/Telstra - Data/;
%let event = event_type.csv;
%let log = log_feature.csv;
%let resource = resource_type.csv;
%let sample = sample_submission.csv;
%let severity = severity_type.csv;
%let test = test.csv;
%let train = train.csv;

/*libname save '\\vmware-host\Shared Folders\Documents\GWU\DNSC 6279\Telstra\';*/
libname save 'C:/Users/Marissa/Documents/_GW MSBA/Data Mining/Kaggle/Telstra - Data/';

** Importing tables;
******************************************************************************;
PROC IMPORT OUT=WORK.EVENT
    FILE="&file_locn.&event"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

PROC IMPORT OUT=WORK.LOG
    FILE="&file_locn.&log"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

PROC IMPORT OUT=WORK.RESOURCE
    FILE="&file_locn.&resource"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

PROC IMPORT OUT=WORK.SAMPLE
    FILE="&file_locn.&sample"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

PROC IMPORT OUT=WORK.SEVERITY
    FILE="&file_locn.&severity"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

PROC IMPORT OUT=WORK.TEST
    FILE="&file_locn.&test"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

PROC IMPORT OUT=WORK.TRAIN
    FILE="&file_locn.&train"
        DBMS=CSV REPLACE;
        GETNAMES=YES;
    DATAROW=2;
    GUESSINGROWS=10;
RUN;

** Count rows in tables;
PROC SQL;
	SELECT COUNT(*),'EVENT' FROM EVENT
	UNION ALL
	SELECT COUNT(*),'LOG' FROM LOG
	UNION ALL
	SELECT COUNT(*),'RESOURCE' FROM RESOURCE
	UNION ALL
	SELECT COUNT(*),'SAMPLE' FROM SAMPLE
	UNION ALL
	SELECT COUNT(*),'SEVERITY' FROM SEVERITY
	UNION ALL
	SELECT COUNT(*),'TEST' FROM TEST
	UNION ALL
	SELECT COUNT(*),'TRAIN' FROM TRAIN;
QUIT;

*Sort data;
PROC SORT DATA = EVENT;
	BY ID EVENT_TYPE;
RUN;
PROC SORT DATA = LOG;
	BY ID LOG_FEATURE VOLUME;
RUN;
PROC SORT DATA = RESOURCE;
	BY ID RESOURCE_TYPE;
RUN;
PROC SORT DATA = SAMPLE;
	BY ID;
RUN;
PROC SORT DATA = SEVERITY;
	BY ID;
RUN;
PROC SORT DATA = TEST;
	BY ID;
RUN;
PROC SORT DATA = TRAIN;
	BY ID;
RUN;

*Find distinct values;
PROC SQL;
	SELECT DISTINCT SEVERITY_TYPE
	FROM SEVERITY;
QUIT;

PROC SQL;
	SELECT DISTINCT EVENT_TYPE
	FROM EVENT;
QUIT;

PROC SQL;
	SELECT DISTINCT RESOURCE_TYPE
	FROM RESOURCE;
QUIT;

PROC SQL;
	SELECT DISTINCT LOCATION
	FROM TEST;
QUIT;

PROC SQL;
	SELECT DISTINCT LOG_FEATURE
	FROM LOG;
QUIT;


proc sql;
/*create table joined_train as select*/
create table joined_train as select
 a.id
,substr(a.location,10) as location
,substr(e.severity_type,15) as severity_type
,substr(c.event_type,12) as event_type
,substr(d.resource_type,15) as resource_type
,substr(b.log_feature,9) as log_feature
,b.volume
,a.fault_severity
from train a
left join log b
on a.id = b.id
left join event c
on a.id = c.id
left join resource d
on a.id = d.id
left join severity e
on a.id = e.id
order by 1,2,3,4,5,6
;

/*create table joined_test as select*/
create table joined_test as select
a.id
,substr(a.location,10) as location
,substr(e.severity_type,15) as severity_type
,substr(c.event_type,12) as event_type
,substr(d.resource_type,15) as resource_type
,substr(b.log_feature,9) as log_feature
,b.volume
from test a
left join log b
on a.id = b.id
left join event c
on a.id = c.id
left join resource d
on a.id = d.id
left join severity e
on a.id = e.id
order by 1,2,3,4,5,6
;
quit;


/*compare levels of categorical variables in each table*/
/*levels that are in test but not train*/
%macro quick_distinct(var, table);
proc sql;
create table distinct_&table._&var. as select distinct
&var.
from joined_&table. /*added joined in front of table*/
order by &var.
;
quit;
%mend;

%macro missing(var);
proc sql;
create table missing_levels_&var. as 
select
 	a.&var.
	,b.&var.
from distinct_test_&var. a
left join distinct_train_&var. b
on a.&var. = b.&var.
where b.&var. is null; /*took out is not null*/
quit;
%mend;

%quick_distinct(location,train);
%quick_distinct(location,test);
%missing(location);

%quick_distinct(severity_type,train);
%quick_distinct(severity_type,test);
%missing(severity_type);

%quick_distinct(event_type,train);
%quick_distinct(event_type,test);
%missing(event_type);

%quick_distinct(resource_type,train);
%quick_distinct(resource_type,test);
%missing(resource_type);

%quick_distinct(log_feature,train);
%quick_distinct(log_feature,test);
%missing(log_feature);


/*Testing to make sure level exist in test but not in train*/
/*
proc sql;
select 'Test', * from joined_test where log_feature = '100'
UNION ALL
select 'Train', * from joined_train where log_feature = '100';
quit;
*/


/*MARISSA ADD - WOE AND EVENT RATE */


proc sql;
create table define_evnt_rt_vars_lf as select
log_feature
,volume
,case 
	when fault_severity = 0 then 1
	else 0
		end as fault_sev_0
,volume*calculated fault_sev_0 as wgtd_fault_sev_0
,case 
	when calculated wgtd_fault_sev_0 = 0 then 1
	else calculated wgtd_fault_sev_0
		end as den_wgtd_fault_sev_0
,case 
	when fault_severity = 1 then 1
	else 0
		end as fault_sev_1
,volume*calculated fault_sev_1 as wgtd_fault_sev_1
,case 
	when calculated wgtd_fault_sev_1 = 0 then 1
	else calculated wgtd_fault_sev_1
		end as den_wgtd_fault_sev_1
,case 
	when fault_severity = 2 then 1
	else 0
		end as fault_sev_2
,volume*calculated fault_sev_2 as wgtd_fault_sev_2

,case 
	when calculated wgtd_fault_sev_2 = 0 then 1
	else calculated wgtd_fault_sev_2
		end as den_wgtd_fault_sev_2

,case 
	when calculated fault_sev_0 = 0 then 1
	else 0
		end as fault_sev_nonevent_0
,case 
	when calculated fault_sev_1 = 0 then 1
	else 0
		end as fault_sev_nonevent_1
,case 
	when calculated fault_sev_2 = 0 then 1
	else 0
		end as fault_sev_nonevent_2

from (select distinct id, log_feature, volume, fault_severity from joined_train)
order by log_feature
;

create table event_rate_table as select
 log_feature
/*uses volume to calculate event rate (these are probably ideal to use)*/
,sum(wgtd_fault_sev_0)/sum(den_wgtd_fault_sev_0) + (rand('NORMAL',.5,1)*.000001) as lf_wgtd_evnt_rt_0
,sum(wgtd_fault_sev_1)/sum(den_wgtd_fault_sev_1) + (rand('NORMAL',.5,1)*.000001) as lf_wgtd_evnt_rt_1
,sum(wgtd_fault_sev_2)/sum(den_wgtd_fault_sev_2) + (rand('NORMAL',.5,1)*.000001) as lf_wgtd_evnt_rt_2

/*take mean of 1s for each event rate severity - will calculate rate not taking into consideration volume*/
,mean(fault_sev_0) + (rand('NORMAL',.5,1)*.000001) as lf_evnt_rt_0
,mean(fault_sev_1) + (rand('NORMAL',.5,1)*.000001) as lf_evnt_rt_1
,mean(fault_sev_2) + (rand('NORMAL',.5,1)*.000001) as lf_evnt_rt_2
from define_evnt_rt_vars_lf
group by 1;

/*checker*/
/*create table event_rate_table_no_rand as select*/
/*log_feature*/
/*uses volume to calculate event rate (these are probably ideal to use)*/
/*,sum(wgtd_fault_sev_0)/sum(den_wgtd_fault_sev_0) as lf_wgtd_evnt_rt_0*/
/*,sum(wgtd_fault_sev_1)/sum(den_wgtd_fault_sev_1) as lf_wgtd_evnt_rt_1*/
/*,sum(wgtd_fault_sev_2)/sum(den_wgtd_fault_sev_2) as lf_wgtd_evnt_rt_2*/

/*take mean of 1s for each event rate severity - will calculate rate not taking into consideration volume*/
/*,mean(fault_sev_0) as lf_evnt_rt_0*/
/*,mean(fault_sev_1) as lf_evnt_rt_1*/
/*,mean(fault_sev_2) as lf_evnt_rt_2*/
/*from define_evnt_rt_vars*/
/*group by 1;*/
quit;


%macro event_rate(var, abbr);
proc sql;
create table define_evnt_rt_&abbr. as select
&var.
,case 
	when fault_severity = 0 then 1
	else 0
		end as fault_sev_0
,case 
	when fault_severity = 1 then 1
	else 0
		end as fault_sev_1
,case 
	when fault_severity = 2 then 1
	else 0
		end as fault_sev_2
,case 
	when calculated fault_sev_0 = 0 then 1
	else 0
		end as fault_sev_nonevent_0
,case 
	when calculated fault_sev_1 = 0 then 1
	else 0
		end as fault_sev_nonevent_1
,case 
	when calculated fault_sev_2 = 0 then 1
	else 0
		end as fault_sev_nonevent_2
from (select distinct id, &var., fault_severity from joined_train)
order by &var.
;
create table event_rate_table_&abbr. as select
&var.
/*take mean of 1s for each event rate severity - will calculate rate not taking into consideration volume*/
,mean(fault_sev_0) + (rand('NORMAL',.5,1)*.000001) as &abbr._evnt_rt_0
,mean(fault_sev_1) + (rand('NORMAL',.5,1)*.000001) as &abbr._evnt_rt_1
,mean(fault_sev_2) + (rand('NORMAL',.5,1)*.000001) as &abbr._evnt_rt_2
from define_evnt_rt_&abbr.
group by 1;
quit;

%mend;
%event_rate(location, loc);




/*set fault severity counts for woe - location*/
proc sql;
create table define_woe_loc as select
 id
,location
,fault_severity
,case 
	when fault_severity = 0 then 1
	else 0
		end as fault_sev_0
,case 
	when fault_severity = 1 then 1
	else 0
		end as fault_sev_1
,case 
	when fault_severity = 2 then 1
	else 0
		end as fault_sev_2
from train
;

/*total counts*/
proc sql; 

	select count(fault_sev_0) into :total_num_event_0
	from define_woe_loc 
	where fault_sev_0 = 1; 

	select count(fault_sev_0) into :total_num_nonevent_0
	from define_woe_loc 
	where fault_sev_0 = 0;

	select count(fault_sev_1) into :total_num_event_1
	from define_woe_loc 
	where fault_sev_1 = 1; 

	select count(fault_sev_1) into :total_num_nonevent_1
	from define_woe_loc 
	where fault_sev_1 = 0;

	select count(fault_sev_2) into :total_num_event_2
	from define_woe_loc 
	where fault_sev_2 = 1; 

	select count(fault_sev_2) into :total_num_nonevent_2
	from define_woe_loc 
	where fault_sev_2 = 0;

quit; 

/*events and non-events for location*/
%put total_num_event=&total_num_event_0; /* 4784 */
%put total_num_nonevent=&total_num_nonevent_0; /* 2597 */
%put total_num_event=&total_num_event_1; /* 1871 */
%put total_num_nonevent=&total_num_nonevent_1; /* 5510 */
%put total_num_event=&total_num_event_2; /* 726 */
%put total_num_nonevent=&total_num_nonevent_2; /* 6655 */

/*sum counts by level for event and nonevent*/
proc sql;
create table grp_location_woe as select
location
,sum(fault_sev_0) as number_event_0
,sum(fault_sev_nonevent_0) as number_nonevent_0
,sum(fault_sev_1) as number_event_1
,sum(fault_sev_nonevent_1) as number_nonevent_1
,sum(fault_sev_2) as number_event_2
,sum(fault_sev_nonevent_0) as number_nonevent_2
from define_evnt_rt_loc
group by 1;
quit;

proc sql;
create table woe_lkup_location as select 
location
/*calculate frequency of event*/
,(number_event_0)/&total_num_event_0. as frequency_of_0
,(number_event_1)/&total_num_event_1. as frequency_of_1
,(number_event_2)/&total_num_event_2. as frequency_of_2
/*calculate frequency of non-event*/
,(number_nonevent_0)/&total_num_event_0. as frequency_of_non0
,(number_nonevent_1)/&total_num_event_1. as frequency_of_non1
,(number_nonevent_2)/&total_num_event_2. as frequency_of_non2
/*calculate woe*/
/*need to use coalesce because log of 0 dne, and if rate 0, then woe = 0*/
,coalesce(100*log((calculated frequency_of_0)/(calculated frequency_of_non0)),0) as woe_loc_0
,coalesce(100*log((calculated frequency_of_1)/(calculated frequency_of_non1)),0) as woe_loc_1
,coalesce(100*log((calculated frequency_of_2)/(calculated frequency_of_non2)),0) as woe_loc_2

from grp_location_woe
;
quit;

/*end help*/


/*set fault severity counts for woe - location*/
proc sql;
create table define_woe_lf as select
 b.id
,a.log_feature
,a.volume
,b.fault_severity
,case 
	when b.fault_severity = 0 then 1
	else 0
		end as fault_sev_0
,a.volume*calculated fault_sev_0 as wgtd_fault_sev_0
,case 
	when calculated fault_sev_0 = 0 then 1
	else 0
		end as fault_sev_nonevent_0
,case 
	when b.fault_severity = 1 then 1
	else 0
		end as fault_sev_1
,a.volume*calculated fault_sev_1 as wgtd_fault_sev_1
,case 
	when calculated fault_sev_1 = 0 then 1
	else 0
		end as fault_sev_nonevent_1
,case 
	when b.fault_severity = 2 then 1
	else 0
		end as fault_sev_2
,a.volume*calculated fault_sev_2 as wgtd_fault_sev_2
,case 
	when calculated fault_sev_2 = 0 then 1
	else 0
		end as fault_sev_nonevent_2
from train b
left join log a 
on a.id = b.id
;

/*total counts*/
proc sql; 

	select count(fault_sev_0) into :total_num_event_lf_0
	from define_woe_lf 
	where fault_sev_0 = 1; 

	select count(fault_sev_0) into :total_num_nonevent_lf_0
	from define_woe_lf
	where fault_sev_0 = 0;

	select count(fault_sev_1) into :total_num_event_lf_1
	from define_woe_lf 
	where fault_sev_1 = 1; 

	select count(fault_sev_1) into :total_num_nonevent_lf_1
	from define_woe_lf 
	where fault_sev_1 = 0;

	select count(fault_sev_2) into :total_num_event_lf_2
	from define_woe_lf 
	where fault_sev_2 = 1; 

	select count(fault_sev_2) into :total_num_nonevent_lf_2
	from define_woe_lf 
	where fault_sev_2 = 0;

quit; 

/*events and non-events for location*/
%put total_num_event_lf=&total_num_event_lf_0; /* 4784 */
%put total_num_nonevent_lf=&total_num_nonevent_lf_0; /* 2597 */
%put total_num_event_lf=&total_num_event_lf_1; /* 1871 */
%put total_num_nonevent_lf=&total_num_nonevent_lf_1; /* 5510 */
%put total_num_event_lf=&total_num_event_lf_2; /* 726 */
%put total_num_nonevent_lf=&total_num_nonevent_lf_2; /* 6655 */

/*sum counts by level for event and nonevent*/
proc sql;
create table grp_lf_woe as select
 log_feature
/*use wgtd_fault_severity as to count non events*/
,sum(wgtd_fault_sev_0) as number_event_0
,sum(fault_sev_nonevent_0) as number_nonevent_0
,sum(wgtd_fault_sev_1) as number_event_1
,sum(fault_sev_nonevent_1) as number_nonevent_1
,sum(wgtd_fault_sev_2) as number_event_2
,sum(fault_sev_nonevent_2) as number_nonevent_2

/*use wgtd_fault_severity as to count non events*/
,sum(fault_sev_0) as lf_evnt_0
,sum(fault_sev_1) as lf_evnt_1
,sum(fault_sev_2) as lf_evnt_2
from define_woe_lf
group by 1;
quit;

proc sql;
create table woe_lkup_lf as select 
log_feature
/*calculate frequency of event*/
,(number_event_0)/&total_num_event_0. as frequency_of_0
,(number_event_1)/&total_num_event_1. as frequency_of_1
,(number_event_2)/&total_num_event_2. as frequency_of_2
/*calculate frequency of non-event*/
,(number_nonevent_0)/&total_num_event_0. as frequency_of_non0
,(number_nonevent_1)/&total_num_event_1. as frequency_of_non1
,(number_nonevent_2)/&total_num_event_2. as frequency_of_non2
/*calculate woe*/
/*need to use coalesce because log of 0 dne, and if rate 0, then woe = 0*/
,coalesce(100*log((calculated frequency_of_0)/(calculated frequency_of_non0)),0) as woe_lf_0
,coalesce(100*log((calculated frequency_of_1)/(calculated frequency_of_non1)),0) as woe_lf_1
,coalesce(100*log((calculated frequency_of_2)/(calculated frequency_of_non2)),0) as woe_lf_2

from grp_lf_woe
;
quit;


/*Eugene start*/

*Create temporary table becuase of substring;
PROC SQL NOPRINT;
	CREATE TABLE TEMP_LOG AS
	SELECT ID, SUBSTR(LOG_FEATURE,9,3) AS LOG_FEATURE
	FROM LOG;
QUIT;

/*attaches event rates to original so that we can add it to our transpose*/
proc sql;
create table log_feature_event_rate as select
a.*
/*used wgtd evnt rt, but we can change this*/
,b.lf_wgtd_evnt_rt_0
,b.lf_wgtd_evnt_rt_1
,b.lf_wgtd_evnt_rt_2
/*join to orig log table because we are redefining input as rate*/
from temp_log a
left join event_rate_table b
on a.log_feature = SUBSTR(b.LOG_FEATURE,9,3);
quit;

proc sql;
create table log_feature_woe as select
a.*
/*used wgtd evnt rt, but we can change this*/
,b.woe_lf_0
,b.woe_lf_1
,b.woe_lf_2
/*join to orig log table because we are redefining input as rate*/
from temp_log a
left join woe_lkup_lf b
on a.log_feature = SUBSTR(b.LOG_FEATURE,9,3)
order by a.id;
quit;



PROC SQL NOPRINT;
	CREATE TABLE TEMP_EVENT AS
	SELECT ID, SUBSTR(EVENT_TYPE,12,2) AS EVENT_TYPE
	FROM EVENT;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE WOE_LOCATION AS
	SELECT ID, SUBSTR(LOCATION,10,3) AS LOCATION, FAULT_SEVERITY
	FROM TRAIN;
QUIT;
proc sql;
create table location_event_rate as select
a.*
/*used wgtd evnt rt, but we can change this*/
,b.loc_evnt_rt_0
,b.loc_evnt_rt_1
,b.loc_evnt_rt_2
/*join to orig log table because we are redefining input as rate*/
from woe_location a
left join event_rate_table_loc b
on a.location = SUBSTR(b.LOCATION,10,3);
quit;

proc sql;
create table location_woe as select
a.*
/*used wgtd evnt rt, but we can change this*/
,b.woe_loc_0
,b.woe_loc_1
,b.woe_loc_2
/*join to orig log table because we are redefining input as rate*/
from woe_location a
left join woe_lkup_location b
on a.location = SUBSTR(b.LOCATION,10,3)
order by a.id;
quit;

/*END MARISSA ADD */






PROC SQL NOPRINT;
	CREATE TABLE WOE_LOG AS
	SELECT * 
	FROM WOE_LOCATION A
	LEFT JOIN TEMP_LOG B ON A.ID = B.ID;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE WOE_EVENT AS
	SELECT * 
	FROM WOE_LOCATION A
	LEFT JOIN TEMP_EVENT B ON A.ID = B.ID;
QUIT;

/*Location WOE*/

proc sql; 
	Create table total_num_count as
	select 'total_num_event_0', count(*)
	from woe_location 
	where fault_severity = 0
	UNION
	select 'total_num_nonevent_0', count(*)
	from woe_location 
	where fault_severity <> 0
	UNION
	select 'total_num_event_1', count(*)
	from woe_location 
	where fault_severity = 1
	UNION
	select 'total_num_nonevent_1', count(*)
	from woe_location 
	where fault_severity <> 1
	UNION
	select 'total_num_event_2', count(*)
	from woe_location 
	where fault_severity = 2 
	UNION
	select 'total_num_nonevent_2', count(*)
	from woe_location 
	where fault_severity <> 2;
quit; 

* calculate frequencies;
proc sql; 
	Create table total_num_count_location as
	select 'total_num_event_0', count(*), location
	from woe_location
	group by location
	where fault_severity = 0




	/*UNION
	select 'total_num_nonevent_0', count(*), location
	from woe_location 
	group by location
	where fault_severity <> 0;


	UNION
	select 'total_num_event_1', count(*)
	from woe_location 
	where fault_severity = 1
	UNION
	select 'total_num_nonevent_1', count(*)
	from woe_location 
	where fault_severity <> 1
	UNION
	select 'total_num_event_2', count(*)
	from woe_location 
	where fault_severity = 2 
	UNION
	select 'total_num_nonevent_2', count(*)
	from woe_location 
	where fault_severity <> 2;
	*/
quit;


























/*Dropping volume column*/

*Create temporary table becuase of substring;
PROC SQL NOPRINT;
	CREATE TABLE TEMP_RESOURCE AS
	SELECT ID, SUBSTR(RESOURCE_TYPE,15,1) AS RESOURCE_TYPE
	FROM RESOURCE;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE TEMP_SEVERITY AS
	SELECT ID, SUBSTR(SEVERITY_TYPE,15,1) AS severity_type
	FROM SEVERITY;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE TEMP_EVENT AS
	SELECT ID, SUBSTR(EVENT_TYPE,12,2) AS EVENT_TYPE
	FROM EVENT;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE TEMP_TEST AS
	SELECT a.ID
	,SUBSTR(a.LOCATION,10,3) AS location
	,b.loc_evnt_rt_0
	,b.loc_evnt_rt_1
	,b.loc_evnt_rt_2
	FROM TEST a
	left join event_rate_table_loc b
	on SUBSTR(a.LOCATION,10,3) = b.location
ORDER BY A.ID;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE TEMP_TRAIN AS
	SELECT a.ID 
	,SUBSTR(a.LOCATION,10,3) AS LOCATION 
	,b.loc_evnt_rt_0
	,b.loc_evnt_rt_1
	,b.loc_evnt_rt_2
	,a.FAULT_SEVERITY
	FROM TRAIN a
left join event_rate_table_loc b
on SUBSTR(a.LOCATION,10,3)=b.location
ORDER BY A.ID;
QUIT;

PROC SQL;
	CREATE TABLE TEMP_LOG AS
	SELECT 
	a.ID, 
	SUBSTR(a.LOG_FEATURE,9,3) AS LOG_FEATURE
	,b.lf_wgtd_evnt_rt_0
	,b.lf_wgtd_evnt_rt_1
	,b.lf_wgtd_evnt_rt_2
/*join to orig log table because we are redefining input as rate*/
from log a
left join event_rate_table b
on SUBSTR(a.LOG_FEATURE,9,3) = b.log_feature
ORDER BY A.ID;
quit;


*Data transposing;
PROC TRANSPOSE DATA=TEMP_RESOURCE OUT=NEW_RESOURCE (DROP=_:) PREFIX=resource_type_;
	BY ID;
	VAR RESOURCE_TYPE;
RUN;

PROC TRANSPOSE DATA=TEMP_EVENT OUT=NEW_EVENT (DROP=_:) PREFIX=event_type_;
	BY ID;
	VAR EVENT_TYPE;
RUN;

PROC TRANSPOSE DATA=TEMP_SEVERITY OUT=NEW_SEVERITY (DROP=_:) PREFIX=severity_type_;
	BY ID;
	VAR SEVERITY_TYPE ;
RUN;

/*proc sort data=log_feature_event_rate;*/
/*by ID ;*/
/*run;*/

PROC TRANSPOSE DATA=TEMP_LOG OUT=NEW_LOG_0 (DROP=_:) PREFIX=log_0_feature_;
	BY ID;
	VAR lf_wgtd_evnt_rt_0;
RUN;
PROC TRANSPOSE DATA=TEMP_LOG OUT=NEW_LOG_1 (DROP=_:) PREFIX=log_1_feature_;
	BY ID;
	VAR lf_wgtd_evnt_rt_1;
RUN;
PROC TRANSPOSE DATA=TEMP_LOG OUT=NEW_LOG_2 (DROP=_:) PREFIX=log_2_feature_;
	BY ID;
	VAR lf_wgtd_evnt_rt_2;
RUN;

/*JOIN TOTAL SUM OF VOLUME FROM ORIG LOG DATASET FOR EACH ID TO TRANSPOSED LOG FEATURE*/
PROC SQL;
CREATE TABLE NEW_LOG2 AS SELECT 
A.ID
,A.LOG_FEATURE
,C.*
,D.*
,E.*
/*,((CASE WHEN log_feature_1 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_2 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_3 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_4 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_5 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_6 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_7 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_8 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_9 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_10 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_11 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_12 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_13 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_14 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_15 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_16 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_17 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_18 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_19 IS NOT NULL THEN 1 ELSE 0 END)*/
/*  + (CASE WHEN log_feature_20 IS NOT NULL THEN 1 ELSE 0 END)*/
/*) AS DIFF_LOG_FEATURES*/
,B.VOLUME AS TOTAL_VOL
FROM TEMP_LOG A
LEFT JOIN (SELECT ID, SUM(VOLUME)AS VOLUME FROM LOG GROUP BY 1) B
ON A.ID = B.ID
LEFT JOIN NEW_LOG_1 C
ON A.ID = C.ID
LEFT JOIN NEW_LOG_2 D
ON A.ID = D.ID
LEFT JOIN NEW_LOG_0 E
ON A.ID = E.ID;
QUIT;


*Combining all the values, summing number of instances for each var;
PROC SQL NOPRINT;
	CREATE TABLE NEW_TEST AS
	SELECT * 
	, ((CASE WHEN c.resource_type_1 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_2 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_3 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_4 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_5 IS NOT NULL THEN 1 ELSE 0 END)
  		) AS DIFF_resource_types
	,((CASE WHEN d.event_type_1 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN d.event_type_2 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_3 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_4 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_5 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_6 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_7 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_8 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_9 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_10 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_11 IS NOT NULL THEN 1 ELSE 0 END)
	  	) AS DIFF_event_types
	FROM TEMP_TEST A
	LEFT JOIN TEMP_SEVERITY B ON A.ID = B.ID
	LEFT JOIN NEW_RESOURCE C ON A.ID = C.ID
	LEFT JOIN NEW_EVENT D ON A.ID = D.ID
	LEFT JOIN NEW_LOG2 E ON A.ID = E.ID;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE NEW_TRAIN AS
	SELECT * 
    , ((CASE WHEN c.resource_type_1 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_2 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_3 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_4 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN c.resource_type_5 IS NOT NULL THEN 1 ELSE 0 END)
  		) AS DIFF_resource_types
	,((CASE WHEN d.event_type_1 IS NOT NULL THEN 1 ELSE 0 END)
      + (CASE WHEN d.event_type_2 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_3 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_4 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_5 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_6 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_7 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_8 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_9 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_10 IS NOT NULL THEN 1 ELSE 0 END)
	  + (CASE WHEN d.event_type_11 IS NOT NULL THEN 1 ELSE 0 END)
	  	) AS DIFF_event_types
	FROM TEMP_TRAIN A
	LEFT JOIN TEMP_SEVERITY B ON A.ID = B.ID
	LEFT JOIN NEW_RESOURCE C ON A.ID = C.ID
	LEFT JOIN NEW_EVENT D ON A.ID = D.ID
	LEFT JOIN NEW_LOG2 E ON A.ID = E.ID;
QUIT;

/*GOING TO HANDLE THIS IN MINER*/

*Changing everything blank to zero;
/*DATA NEW_TEST;*/
/*	SET NEW_TEST;*/
/*	ARRAY ZERO LOCATION SEVERITY_TYPE RESOURCE_TYPE_1 RESOURCE_TYPE_2 RESOURCE_TYPE_3 RESOURCE_TYPE_4 RESOURCE_TYPE_5 EVENT_TYPE_1 EVENT_TYPE_2 EVENT_TYPE_3 EVENT_TYPE_4 EVENT_TYPE_5 EVENT_TYPE_6 EVENT_TYPE_7 EVENT_TYPE_8 EVENT_TYPE_9 EVENT_TYPE_10 EVENT_TYPE_11 LOG_FEATURE_1 LOG_FEATURE_2  LOG_FEATURE_3 LOG_FEATURE_4 LOG_FEATURE_5 LOG_FEATURE_6 LOG_FEATURE_7 LOG_FEATURE_8 LOG_FEATURE_9 LOG_FEATURE_10 LOG_FEATURE_11 LOG_FEATURE_12 LOG_FEATURE_13 LOG_FEATURE_14 LOG_FEATURE_15 LOG_FEATURE_16 LOG_FEATURE_17 LOG_FEATURE_18 LOG_FEATURE_19 LOG_FEATURE_20;*/
/*	DO OVER ZERO;*/
/*		IF ZERO=. THEN ZERO=0;*/
/*	END;*/
/*RUN;*/
/**/
/*DATA NEW_TRAIN;*/
/*	SET NEW_TRAIN;*/
/*	ARRAY ZERO LOCATION SEVERITY_TYPE RESOURCE_TYPE_1 RESOURCE_TYPE_2 RESOURCE_TYPE_3 RESOURCE_TYPE_4 RESOURCE_TYPE_5 EVENT_TYPE_1 EVENT_TYPE_2 EVENT_TYPE_3 EVENT_TYPE_4 EVENT_TYPE_5 EVENT_TYPE_6 EVENT_TYPE_7 EVENT_TYPE_8 EVENT_TYPE_9 EVENT_TYPE_10 EVENT_TYPE_11 LOG_FEATURE_1 LOG_FEATURE_2  LOG_FEATURE_3 LOG_FEATURE_4 LOG_FEATURE_5 LOG_FEATURE_6 LOG_FEATURE_7 LOG_FEATURE_8 LOG_FEATURE_9 LOG_FEATURE_10 LOG_FEATURE_11 LOG_FEATURE_12 LOG_FEATURE_13 LOG_FEATURE_14 LOG_FEATURE_15 LOG_FEATURE_16 LOG_FEATURE_17 LOG_FEATURE_18 LOG_FEATURE_19 LOG_FEATURE_20;*/
/*	DO OVER ZERO;*/
/*		IF ZERO=. THEN ZERO=0;*/
/*	END;*/
/*RUN;*/

*Limiting the zero fault severity to half the sample;
PROC SQL NOPRINT;
	CREATE TABLE OVER_SAMPLE_ZERO AS
	SELECT *
	FROM NEW_TRAIN
	WHERE FAULT_SEVERITY = 0;
QUIT;

PROC SURVEYSELECT DATA = OVER_SAMPLE_ZERO
	METHOD = SRS N = 2392 OUT = NEW_OVER_SAMPLE_ZERO;
RUN;

PROC SQL NOPRINT;
	CREATE TABLE NEW_TRAIN_SAMPLE AS
	SELECT *
	FROM NEW_TRAIN
	WHERE FAULT_SEVERITY = 1 OR FAULT_SEVERITY = 2
	UNION
	SELECT *
	FROM NEW_OVER_SAMPLE_ZERO;
QUIT;



/*save files as sas datasets*/
proc sql;
create table save.new_train as select * from new_train;
create table save.new_train_sample as select * from new_train_sample;
create table save.new_test as select * from new_test;
quit;

*Export to CSV;
/*PROC EXPORT*/
/*	DATA = NEW_TEST*/
/*	OUTFILE = "&file_locn.new_test.csv"*/
/*	DBMS = CSV*/
/*	REPLACE;*/
/*RUN;*/
/**/
/*PROC EXPORT*/
/*	DATA = NEW_TRAIN*/
/*	OUTFILE = "&file_locn.new_train.csv"*/
/*	DBMS = CSV*/
/*	REPLACE;*/
/*RUN;*/
/**/
/*PROC EXPORT*/
/*	DATA = NEW_TRAIN_SAMPLE*/
/*	OUTFILE = "&file_locn.new_train_sample.csv"*/
/*	DBMS = CSV*/
/*	REPLACE;*/
/*RUN;*/
/**/
