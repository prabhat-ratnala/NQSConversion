drop table if exists sbi_pres_temp.pd_site_detail;
create table if not exists sbi_pres_temp.pd_site_detail(
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

insert into table sbi_pres_temp.pd_site_detail
select distinct
all_a.hrow_id,
all_a.project_id,
all_a.int_site_id,
case when all_a.data_source_disp = 'MC' then ds.sponsor_ref_site_nbr
when all_a.data_source_disp = 'IC' and ds.sponsor_ref_site_nbr is NOT NULL then ds.sponsor_ref_site_nbr
when all_a.data_source_disp = 'IC' and ds.sponsor_ref_site_nbr is NULL then cast(ds.site_id as varchar(100))
end as sponsor_ref_site_nbr,
all_a.int_pi_id,
all_a.country_id,
all_a.pi_name,
case when all_a.site_name like ('(%') then all_a.site_name
else REGEXP_REPLACE(all_a.site_name, '^[! - /]+', '') end as site_name,
-- ltrim(all_a.site_name) as site_name,
all_a.event_type,
all_a.data_source_disp,
all_a.data_source,
all_a.event_date,
all_a.last_processed_date,
-- all_a.last_enrollment_date,
all_a.high,
all_a.low,
all_a.ceiling,
all_a.value,
all_a.planned_date,
-- all_a.visit_type,
all_a.visit_id,
all_a.visit_mode,
all_a.reviewer,
all_a.visit_start_date,
all_a.visit_end_date,
all_a.sys_update_date,
all_a.days_on_site,
all_a.monitor,
all_a.report_submission_date,
all_a.report_creation_date,
all_a.report_completion_date
from
(
select
q4.hrow_id,
q4.project_id as project_id,
q4.int_site_id as int_site_id,
int_pi_id,
q4.country_id as country_id,
full_name as pi_name,
ltrim(site_name) as site_name,
event_type,
q4.data_source as data_source_disp,
case
when q4.data_source = 'MC' then 'Medidata CTMS'
when q4.data_source = 'IC' then 'Impact CTMS'
END as data_source,
q4.event_date,
last_processed_date,
-- le.last_enrollment_date,
q4.high,
q4.low,
q4.ceiling,
q4.value,
q4.planned_date,
q4.visit_id,
q4.visit_mode,
q4.reviewer,
q4.visit_start_date,
q4.visit_end_date,
q4.sys_update_date,
q4.days_on_site,
q4.monitor,
q4.report_submission_date,
q4.report_creation_date,
q4.report_completion_date
from (
select
e.hrow_id,
e.project_id,
e.event_date,
e.event_type,
e.country_id,
e.int_site_id,
e.last_processed_date,
e.data_source,
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
from sbi_star.f_sites e inner join sbi_star.sbi_processed_date p on (e.last_processed_date = p.last_process_date)) q4
left join (select distinct case when c.first_name is null then c.last_name
else concat(c.last_name,', ', c.first_name) end as full_name, f.facility_name as site_name, q2.int_site_id as int_site_id, q2.int_pi_id as int_pi_id
from (select q1.int_site_id, q1.int_pi_id,q1.event_source from (
select int_site_id, int_pi_id,event_date,event_source,
rank() over (partition by int_site_id order by event_date desc) as updated_rank
from sbi_raw.unique_dsites where active_flag='Y') q1 where updated_rank =1) q2
left outer  join analysis_presentation.d_contact c on q2.int_pi_id=c.int_contact_id and q2.event_source = c.event_source
left outer join analysis_presentation.d_site_facility sf on (q2.int_site_id=sf.int_site_id and sf.primary_flag = 'Y' and sf.active_flag = 'Y')
left outer join analysis_presentation.d_facility f on sf.int_facility_id=f.int_facility_id) s
on (q4.int_site_id = s.int_site_id)
UNION ALL
select distinct
sv.hrow_id,
sv.project_id,
sv.int_site_id,
s.int_pi_id,
sv.country_cd as country_id,
s.full_name as pi_name,
s.site_name,
case when sv.visit_type = 'SIV' then 'SIV'
when sv.visit_type = 'SQV' then 'PSSV'
when sv.visit_type = 'SCV' then 'COV'
when sv.visit_type = 'SMC' then 'SMC'
when sv.visit_type = 'SMV' then 'IMV'
end as event_type,
sv.data_source as data_source_disp,
case
 when sv.data_source = 'MC' then 'Medidata CTMS'
when sv.data_source = 'IC' then 'Impact CTMS'
 END as data_source,
sv.event_date,
lpd.last_process_date as last_processed_date,
-- NULL as last_enrollment_date,
NULL as high,
NULL as low,
NULL as ceiling,
NULL as value,
sv.event_date as planned_date,
sv.visit_id as visit_id,
fsv.visit_mode as visit_mode,
concat(rv.first_name,' ', rv.last_name) as reviewer,
fsv.visit_start_date,
fsv.visit_end_date,
fsv.sys_update_date,
fsv.dos,
concat(dr.first_name,' ', dr.last_name) as monitor,
fsv.first_draft as report_submission_date,
fsv.mvr_created as report_creation_date,
fsv.mvr_completion_date as report_completion_date
from sbi_star.f_sites_visits sv
left join
analysis_presentation.f_site_visit fsv
on (sv.project_id = fsv.project_code and sv.int_site_id = fsv.int_site_id and sv.visit_id = fsv.int_site_visit_id)
inner join sbi_star.sbi_processed_date lpd
on (sv.last_processed_date = lpd.last_process_date)
left join analysis_presentation.d_resource dr on (fsv.visit_monitoring_cra = dr.int_resource_id)
left join analysis_presentation.d_resource rv on (fsv.visit_approved_by_id	 = rv.int_resource_id)
left join (select distinct case when c.first_name is null then c.last_name
else concat(c.last_name,', ', first_name) end as full_name, f.facility_name as site_name, q2.int_site_id as int_site_id, q2.int_pi_id as int_pi_id 
   from (select q1.int_site_id, q1.int_pi_id, q1.event_source from (
   select int_site_id, int_pi_id,event_date,event_source,
   rank() over (partition by int_site_id order by event_date desc) as updated_rank
   from sbi_raw.unique_dsites where active_flag='Y') q1 where updated_rank =1) q2
   left outer  join analysis_presentation.d_contact c on q2.int_pi_id=c.int_contact_id and q2.event_source = c.event_source
   left outer join analysis_presentation.d_site_facility sf on (q2.int_site_id=sf.int_site_id and sf.primary_flag = 'Y' and sf.active_flag = 'Y')
   left outer join analysis_presentation.d_facility f on sf.int_facility_id=f.int_facility_id) s
on (sv.int_site_id = s.int_site_id)
where sv.data_source in ('MC','IC')
) all_a
-- left join
-- (select project_code, site_id,int_site_id, sponsor_ref_site_nbr from analysis_presentation.d_site  where event_source = 'IMPACT'
-- UNION ALL
-- select project_code,site_id, int_site_id, sponsor_ref_site_nbr from analysis_presentation.d_site ds where event_source = 'MA'
-- and status!= 'Created in IMPACT'
-- UNION ALL
-- select project_code,site_id, int_site_id, sponsor_ref_site_nbr from analysis_presentation.d_site ds where event_source = 'MA'
-- and status IS NULL ) ds
-- on (all_a.project_id = ds.project_code and all_a.int_site_id = ds.int_site_id)
join sbi_raw.unique_dsites ds
on (ds.project_code = all_a.project_id and ds.int_site_id = all_a.int_site_id);