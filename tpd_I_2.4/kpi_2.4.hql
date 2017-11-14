drop table if exists sbi_raw.kpi_raw;
create table if not exists sbi_raw.kpi_raw(
project_code varchar(50),
event_type varchar(100),
event_value varchar(20),
event_date timestamp,
comments varchar(4000),
comments_updated timestamp,
last_processed_date timestamp)
stored as orc tblproperties ("orc.compress"="SNAPPY");


with xp AS
( 
  select 'Wbs106'  alt_str , 'pl_overall_comment' cmnt_fld 
  union ALL
  select 'Wbs72' alt_str, 'quality_sdv_comment' as cmnt_fld
  union ALL
  select 'Wbs73' alt_str, 'quality_mvr_comment' as cmnt_fld
  union ALL
  select 'Wbs74' alt_str, 'quality_query_commen' as cmnt_fld
  union ALL
  select 'Wbs80' alt_str, 'quality_time_comment' as cmnt_fld
  union ALL
  select 'Wbs81' alt_str, 'quality_tmf_comment' as cmnt_fld
  union ALL
  select 'Wbs82' alt_str, 'quality_risk_comment' as cmnt_fld
  union ALL
  select 'Wbs107' alt_str, 'quality_issue_comm' as cmnt_fld
  union ALL
  select 'Wbs108' alt_str, 'vendor_comment' as cmnt_fld
  union ALL
  select 'Wbs84' alt_str, 'budget_scope_comment' as cmnt_fld
  union ALL
  select 'Wbs85' alt_str, 'budget_co_comment' as cmnt_fld
  union ALL
  select 'Wbs86' alt_str, 'budget_gpplan_commen' as cmnt_fld
  union ALL
  select 'Wbs88' alt_str, 'budget_invest_commen' as cmnt_fld
  union ALL
  select 'Wbs92' alt_str, 'sched_patient_commen' as cmnt_fld
  union ALL
  select 'Wbs93' alt_str, 'sched_sites_comment' as cmnt_fld
  union ALL
  select 'Wbs109' alt_str, 'country_reg_sub_comm' as cmnt_fld
  union ALL
  select 'Wbs95' alt_str, 'resource_int_comment' as cmnt_fld
  union ALL
  select 'Wbs709' alt_str, '' as cmnt_fld
--   order by 1
),
kpi as (
select distinct 
       ah.alt_structure,
       coalesce ( cd.inc_project_code, pe.short_name) project_code , 
       xp.Master_proj_id as Master_proj_id,
       s.description event_type,
       ev.description event_value,
       ah.action_date event_date,
       'PV' data_source
 from raw_pv_c_int.custom_data cd
     join raw_pv_c_int.planning_entity pe
     on pe.planning_code = cd.planning_code
	 join raw_pv_c_int.attribute_history ah
	 on ah.structure_code = cd.planning_code
	 join raw_pv_c_int.structure s on s.structure_code  = ah.alt_structure
       join sbi_presentation.pd_int_project_mapping xp 
            ON ( xp.child_source = 'PV' AND xp.child_proj_id =  coalesce ( cd.inc_project_code, pe.short_name) )
     join (
            select max(ahi.action_date) action_date, ahi.structure_code, ahi.alt_structure from raw_pv_c_int.attribute_history ahi
                   group by ahi.structure_code, ahi.alt_structure ) ahi 
                   on ( ahi.structure_code = cd.planning_code and ahi.alt_structure = s.structure_code 
                        and ahi.action_date = ah.action_date )
     left join raw_pv_c_int.structure ev on ev.structure_code = ah.new_value
                           
                          ),
cmnt AS
(
select distinct cd.inc_project_code project_code, l.key1 comment_field, l.line_text comments, l.last_updated_on from raw_pv_c_int.long_text  l
join raw_pv_c_int.custom_data cd
on l.key2 = cd.planning_code
join ( 
select cd.inc_project_code, l1.key1 as comment_field, max(l1.last_updated_on) last_Updated_on  from raw_pv_c_int.long_text l1 join raw_pv_c_int.custom_data cd
on l1.key2 = cd.planning_code group by cd.inc_project_code, l1.key1 
) l1 on l1.inc_project_code = cd.inc_project_code and l1.comment_field = l.key1 and l.last_updated_on = l1.last_updated_on
where l.key1 in ( 'resource_int_comment', 'pl_overall_comment',
                  'quality_sdv_comment', 'quality_mvr_comment',
				  'quality_query_commen', 'quality_time_comment',
				  'quality_tmf_comment', 'quality_risk_comment',
				  'quality_issue_comm', 'vendor_comment',
				  'budget_scope_comment', 'budget_co_comment',
				  'budget_gpplan_commen', 'budget_invest_commen',
				  'sched_patient_commen', 'sched_sites_comment',
				  'country_reg_sub_comm', 'resource_int_comment' )
 )               
 insert overwrite table sbi_raw.kpi_raw
select kpi.project_code, -- kpi.alt_structure, 
kpi.event_type, kpi.event_value, kpi.event_date , cmnt.comments, cmnt.last_updated_on comments_updated , lpd.last_processed_date 
from xp
  join kpi on xp.alt_str = kpi.alt_structure
 left join cmnt on cmnt.project_code = kpi.project_code and cmnt.comment_field = xp.cmnt_fld 
 join cdm_star.sbi_processed_date lpd;
 
 
 -- category table ----
 
 drop table if exists sbi_raw.kpi_category_raw;
create table sbi_raw.kpi_category_raw as
select a.project_code, kpi_group, event_value, event_date from
(
select project_code,
'SCHEDULE' AS kpi_group,
case when max(number) = 1 then 'GREEN'
when max(number) = 2 then 'AMBER'
when max(number) = 3 then 'RED'
end as event_value

FROM sbi_raw.kpi_raw kr
join
sbi_raw.kpi_colors kc
on (lower(kr.event_value) = lower(kc.color))
WHERE  event_type IN ( 'Country Regulatory Submission','Subject Enrollment Tracking','Site Activation Tracking','Internal Resource Impact' )
group by project_code
) a
left join
(
SELECT project_code,
greatest(coalesce(Max(event_date), CAST('1900-01-01' as timestamp)), coalesce (max(comments_updated), CAST('1900-01-01' as timestamp)))
as event_date FROM sbi_raw.kpi_raw
WHERE  event_type IN ( 'Country Regulatory Submission','Subject Enrollment Tracking','Site Activation Tracking','Internal Resource Impact' )
group by project_code
)b
on (a.project_code = b.project_code)


UNION ALL

select a.project_code, kpi_group, event_value, event_date from
(
select project_code,
'OVERALL' AS kpi_group,
case when max(number) = 1 then 'GREEN'
when max(number) = 2 then 'AMBER'
when max(number) = 3 then 'RED'
end as event_value

FROM sbi_raw.kpi_raw kr
join
sbi_raw.kpi_colors kc
on (lower(kr.event_value) = lower(kc.color))
WHERE  event_type IN ( 'Calculated Overall RAG Assessment','PL Overall RAG Assessment')
group by project_code
) a
left join
(
SELECT project_code,
greatest(coalesce(Max(event_date), CAST('1900-01-01' as timestamp)), coalesce (max(comments_updated), CAST('1900-01-01' as timestamp)))
as event_date FROM sbi_raw.kpi_raw
WHERE  event_type IN ( 'Calculated Overall RAG Assessment','PL Overall RAG Assessment')
group by project_code
)b
on (a.project_code = b.project_code)



UNION ALL

select a.project_code, kpi_group, event_value, event_date from
(
select project_code,
'OVERALL' AS kpi_group,
case when max(number) = 1 then 'GREEN'
when max(number) = 2 then 'AMBER'
when max(number) = 3 then 'RED'
end as event_value

FROM sbi_raw.kpi_raw kr
join
sbi_raw.kpi_colors kc
on (lower(kr.event_value) = lower(kc.color))
WHERE  event_type IN ( 'Calculated Overall RAG Assessment','PL Overall RAG Assessment')
group by project_code
) a
left join
(
SELECT project_code,
greatest(coalesce(Max(event_date), CAST('1900-01-01' as timestamp)), coalesce (max(comments_updated), CAST('1900-01-01' as timestamp)))
as event_date FROM sbi_raw.kpi_raw
WHERE  event_type IN ( 'Calculated Overall RAG Assessment','PL Overall RAG Assessment')
group by project_code
)b
on (a.project_code = b.project_code)


UNION ALL

select a.project_code, kpi_group, event_value, event_date from
(
select project_code,
'QUALITY' AS kpi_group,
case when max(number) = 1 then 'GREEN'
when max(number) = 2 then 'AMBER'
when max(number) = 3 then 'RED'
end as event_value

FROM sbi_raw.kpi_raw kr
join
sbi_raw.kpi_colors kc
on (lower(kr.event_value) = lower(kc.color))
WHERE  event_type IN ( 'Monitoring Visit Reports Aging','Query Aging','Site Data Entry','Service Delivery Risk',
'Trial Master File (TMF) Completeness','Quality Issue','Vendor','Source Data Verification & Monitoring')
group by project_code
) a
left join
(
SELECT project_code,
greatest(coalesce(Max(event_date), CAST('1900-01-01' as timestamp)), coalesce (max(comments_updated), CAST('1900-01-01' as timestamp)))
as event_date FROM sbi_raw.kpi_raw
WHERE  event_type IN ( 'Monitoring Visit Reports Aging','Query Aging','Site Data Entry','Service Delivery Risk',
'Trial Master File (TMF) Completeness','Quality Issue','Vendor','Source Data Verification & Monitoring')
group by project_code
)b
on (a.project_code = b.project_code)


UNION ALL

select a.project_code, kpi_group, event_value, event_date from
(
select project_code,
'RESOURCE' AS kpi_group,
case when max(number) = 1 then 'GREEN'
when max(number) = 2 then 'AMBER'
when max(number) = 3 then 'RED'
end as event_value

FROM sbi_raw.kpi_raw kr
join
sbi_raw.kpi_colors kc
on (lower(kr.event_value) = lower(kc.color))
WHERE  event_type IN ('Internal Resource Impact')
group by project_code
) a
left join
(
SELECT project_code,
greatest(coalesce(Max(event_date), CAST('1900-01-01' as timestamp)), coalesce (max(comments_updated), CAST('1900-01-01' as timestamp)))
as event_date FROM sbi_raw.kpi_raw
WHERE  event_type IN ('Internal Resource Impact')
group by project_code
)b
on (a.project_code = b.project_code);


-- KPI Presentation

drop table if exists sbi_presentation.pd_project_kpi;
create table if not exists sbi_presentation.pd_project_kpi(
project_code varchar(50),
kpi_group varchar(50),
event_type varchar(100),
event_date timestamp,
event_value varchar(20),
RAG_COLOR_NUM int,
RAG_COLOR_char varchar(3),
comments varchar(4000),
comments_updated timestamp,
last_processed_date timestamp)
stored as orc tblproperties ("orc.compress"="SNAPPY");
insert into table sbi_presentation.pd_project_kpi
select 
kpi_raw.project_code as project_code,
case when event_type IN ( 'Country Regulatory Submission','Subject Enrollment Tracking','Site Activation Tracking','Internal Resource Impact' ) then 'SCHEDULE'
when event_type IN ( 'Calculated Overall RAG Assessment','PL Overall RAG Assessment') then 'OVERALL'
when event_type IN ( 'Monitoring Visit Reports Aging','Query Aging','Site Data Entry','Service Delivery Risk',
'Trial Master File (TMF) Completeness','Quality Issue','Vendor','Source Data Verification & Monitoring') then 'QUALITY'
when event_type IN ( 'Initial Agreement Status','Change Order Status', 'Gross Profit Percentage To Date','Investigator Payments') then 'BUDGET'
when event_type IN ('Internal Resource Impact') then 'RESOURCE'
-- when kpi_category.kpi_group in ('OVERALL','QUALITY','BUDGET','SCHEDULE','RESOURCE') then 'CATEGORY'
end as KPI_GROUP,
event_type,
event_date,
kpi_raw.event_value as event_value,
case when lower(kpi_raw.event_value) = 'green' then '1'
when lower(kpi_raw.event_value) = 'amber' then '2'
when lower(kpi_raw.event_value) = 'red' then '3'
else '4' end as RAG_COLOR_NUM,
case when lower(kpi_raw.event_value) = 'green'  then 'G'
when lower(kpi_raw.event_value) = 'amber' then 'A'
when lower(kpi_raw.event_value) = 'red' then 'R'
else 'N/A' end as RAG_COLOR_CHAR,
comments,
comments_updated,
last_processed_date
from sbi_raw.kpi_raw kpi_raw


union all
select DISTINCT
kpi_category.project_code as project_code,
case when kpi_category.kpi_group in ('OVERALL','QUALITY','BUDGET','SCHEDULE','RESOURCE') then 'CATEGORY'
end as KPI_GROUP,
kpi_category.kpi_group as event_type,
event_date,
event_value,
case when lower(event_value) = 'green' then '1'
when lower(event_value) = 'amber' then '2'
when lower(event_value) = 'red' then '3'
else '4' end as RAG_COLOR_NUM,
case when lower(event_value) = 'green'  then 'G'
when lower(event_value) = 'amber' then 'A'
when lower(event_value) = 'red' then 'R'
else 'N/A' end as RAG_COLOR_CHAR,
'N/A' as comments,
cast('' as timestamp) as comments_updated,
lpd.last_process_date  as last_processed_date

from 
sbi_raw.kpi_category_raw kpi_category 
join sbi_star.sbi_processed_date lpd;

