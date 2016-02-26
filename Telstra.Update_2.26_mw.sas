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
	SELECT DISTINCT VOLUME
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
%macro quick_distinct(var, table);
proc sql;
create table distinct_&table._&var. as select distinct
&var.
from &table.
order by &var.
;
quit;
%mend;
%quick_distinct(location,train);
%quick_distinct(location,test);
%macro missing(var);
proc sql;
create table missing_levels_&var. as select
 a.&var.
,b.&var.
from distinct_test_&var. a
left join distinct_train_&var. b
on a.&var. = b.&var.
where b.&var. is not null;
quit;
%mend;

%missing(location);


proc sql;
create table define_evnt_rt_vars as select
log_feature
,volume
,case 
	when fault_severity = 0 then 1
	else 0
		end as fault_sev_0
,volume*calculated fault_sev_0 as wgtd_fault_sev_0
,case 
	when fault_severity = 1 then 1
	else 0
		end as fault_sev_1
,volume*calculated fault_sev_1 as wgtd_fault_sev_1
,case 
	when fault_severity = 2 then 1
	else 0
		end as fault_sev_2
,volume*calculated fault_sev_2 as wgtd_fault_sev_2
from joined_train
;

create table event_rate_table as select
log_feature
/*not sure if we want to make some sort of overall distribution to incorporate volume*/
,sum(wgtd_fault_sev_0)as lf_evnt_vol_0
,sum(wgtd_fault_sev_1)as lf_evnt_vol_1
,sum(wgtd_fault_sev_2)as lf_evnt_vol_2
/*take mean of 1s for each event rate severity - will calculate rate*/
,mean(fault_sev_0) as lf_evnt_rt_0
,mean(fault_sev_1) as lf_evnt_rt_1
,mean(fault_sev_2) as lf_evnt_rt_2
from define_evnt_rt_vars
group by 1;

/*attaches event rates to original so that we can add it to our transpose*/
create table log_feature_event_rate as select
a.*
,b.lf_evnt_rt_0
,b.lf_evnt_rt_1
,b.lf_evnt_rt_2
from log a
left join event_rate_table b
on a.log_feature = b.log_feature;
quit;



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
	SELECT ID, SUBSTR(LOCATION,10,3) AS location
	FROM TEST;
QUIT;

PROC SQL NOPRINT;
	CREATE TABLE TEMP_TRAIN AS
	SELECT ID, SUBSTR(LOCATION,10,3) AS LOCATION, FAULT_SEVERITY
	FROM TRAIN;
QUIT;

/*Dropping volume column*/
PROC SQL NOPRINT;
	CREATE TABLE TEMP_LOG AS
	SELECT ID, SUBSTR(LOG_FEATURE,9,3) AS LOG_FEATURE
	FROM LOG;
QUIT;

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

PROC TRANSPOSE DATA=TEMP_LOG OUT=NEW_LOG (DROP=_:) PREFIX=log_feature_;
	BY ID;
	VAR LOG_FEATURE;
RUN;

/*JOIN TOTAL SUM OF VOLUME FROM ORIG LOG DATASET FOR EACH ID TO TRANSPOSED LOG FEATURE*/
PROC SQL;
CREATE TABLE NEW_LOG2 AS SELECT 
A.*
,((CASE WHEN log_feature_1 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_2 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_3 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_4 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_5 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_6 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_7 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_8 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_9 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_10 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_11 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_12 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_13 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_14 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_15 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_16 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_17 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_18 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_19 IS NOT NULL THEN 1 ELSE 0 END)
  + (CASE WHEN log_feature_20 IS NOT NULL THEN 1 ELSE 0 END)
) AS DIFF_LOG_FEATURES
,B.VOLUME AS TOTAL_VOL
FROM NEW_LOG A
LEFT JOIN (SELECT ID, SUM(VOLUME)AS VOLUME FROM LOG GROUP BY 1) B
ON A.ID = B.ID;
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

*Changing everything blank to zero;
DATA NEW_TEST;
	SET NEW_TEST;
	ARRAY ZERO LOCATION SEVERITY_TYPE RESOURCE_TYPE_1 RESOURCE_TYPE_2 RESOURCE_TYPE_3 RESOURCE_TYPE_4 RESOURCE_TYPE_5 EVENT_TYPE_1 EVENT_TYPE_2 EVENT_TYPE_3 EVENT_TYPE_4 EVENT_TYPE_5 EVENT_TYPE_6 EVENT_TYPE_7 EVENT_TYPE_8 EVENT_TYPE_9 EVENT_TYPE_10 EVENT_TYPE_11 LOG_FEATURE_1 LOG_FEATURE_2  LOG_FEATURE_3 LOG_FEATURE_4 LOG_FEATURE_5 LOG_FEATURE_6 LOG_FEATURE_7 LOG_FEATURE_8 LOG_FEATURE_9 LOG_FEATURE_10 LOG_FEATURE_11 LOG_FEATURE_12 LOG_FEATURE_13 LOG_FEATURE_14 LOG_FEATURE_15 LOG_FEATURE_16 LOG_FEATURE_17 LOG_FEATURE_18 LOG_FEATURE_19 LOG_FEATURE_20;
	DO OVER ZERO;
		IF ZERO=. THEN ZERO=0;
	END;
RUN;

DATA NEW_TRAIN;
	SET NEW_TRAIN;
	ARRAY ZERO LOCATION SEVERITY_TYPE RESOURCE_TYPE_1 RESOURCE_TYPE_2 RESOURCE_TYPE_3 RESOURCE_TYPE_4 RESOURCE_TYPE_5 EVENT_TYPE_1 EVENT_TYPE_2 EVENT_TYPE_3 EVENT_TYPE_4 EVENT_TYPE_5 EVENT_TYPE_6 EVENT_TYPE_7 EVENT_TYPE_8 EVENT_TYPE_9 EVENT_TYPE_10 EVENT_TYPE_11 LOG_FEATURE_1 LOG_FEATURE_2  LOG_FEATURE_3 LOG_FEATURE_4 LOG_FEATURE_5 LOG_FEATURE_6 LOG_FEATURE_7 LOG_FEATURE_8 LOG_FEATURE_9 LOG_FEATURE_10 LOG_FEATURE_11 LOG_FEATURE_12 LOG_FEATURE_13 LOG_FEATURE_14 LOG_FEATURE_15 LOG_FEATURE_16 LOG_FEATURE_17 LOG_FEATURE_18 LOG_FEATURE_19 LOG_FEATURE_20;
	DO OVER ZERO;
		IF ZERO=. THEN ZERO=0;
	END;
RUN;

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
PROC EXPORT
	DATA = NEW_TEST
	OUTFILE = "&file_locn.new_test.csv"
	DBMS = CSV
	REPLACE;
RUN;

PROC EXPORT
	DATA = NEW_TRAIN
	OUTFILE = "&file_locn.new_train.csv"
	DBMS = CSV
	REPLACE;
RUN;

PROC EXPORT
	DATA = NEW_TRAIN_SAMPLE
	OUTFILE = "&file_locn.new_train_sample.csv"
	DBMS = CSV
	REPLACE;
RUN;
