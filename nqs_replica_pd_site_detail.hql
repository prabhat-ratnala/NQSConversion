drop table if exists nqs_replica.pd_site_detail;
create table if not exists nqs_replica.pd_site_detail(
hrow_id  varchar(256)
,project_id  varchar(15)
,int_site_id  varchar(31)
,sponsor_ref_site_nbr varchar(64)
,int_pi_id      int
,country_id  varchar(3)
,pi_name   varchar(37)
,site_name      varchar(60)
,event_type  varchar(60)
,data_source_disp varchar(5)
,data_source varchar(32)
,event_date     timestamp
,last_processed_date timestamp
-- ,last_enrollment_date timestamp
,high   int
,low   int
,ceiling   int
,value   int
,planned_date date
,visit_id  varchar(64)
,visit_mode varchar(10) -- new
,reviewer varchar(50) -- new
,visit_start_date timestamp
,visit_end_date timestamp
,sys_update_date timestamp
,days_on_site varchar(10)
,monitor varchar(1000)
,report_submission_date timestamp
,report_creation_date timestamp
,report_completion_date timestamp
) STORED AS ORC TBLPROPERTIES ("orc.compress"="SNAPPY");

insert into table nqs_replica.pd_site_detail

select 
concat_ws('.',fs.event_type,fs.project_id,fs.int_site_id,fs.country_id) as hrow_id,
fs.project_id, 
fs.int_site_id,
site.site_number as sponsor_ref_site_nbr,
site.pi_int_id as int_pi_id,
fs.country_id,
case when site.pi_first_name is null then site.pi_last_name
else concat(site.pi_last_name,', ', site.pi_first_name) end as pi_name,
case when site.facility_name like ('(%') then site.facility_name
else REGEXP_REPLACE(site.facility_name, '^[! - /]+', '') end as site_name,
fs.event_type,
fs.data_source as data_source_disp,
case
when fs.data_source = 'MC' then 'Medidata CTMS'
when fs.data_source = 'IC' then 'Impact CTMS'
END as data_source,
fs.event_date,
lpd.last_process_date as last_processed_date,
NULL as high,
NULL as low,
NULL as ceiling,
NULL as value,
NULL as planned_date,
NULL as visit_id,
NULL as visit_mode,
NULL as reviewer,
NULL as visit_start_date,
NULL as visit_end_date,
NULL as sys_update_date,
NULL as days_on_site,
NULL as monitor,
NULL as report_submission_date,
NULL as report_creation_date,
NULL as report_completion_date
from sbi_star.f_sites fs
left join cdm.site site
on (fs.project_id = site.project_code and fs.int_site_id = site.int_site_id)
join sbi_star.sbi_processed_date lpd
on (lpd.last_process_date = fs.last_processed_date);



