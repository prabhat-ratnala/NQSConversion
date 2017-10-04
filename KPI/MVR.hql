-- MVR AGING
drop table if exists sbi_raw.mvr_aging;
create table if not exists sbi_raw.mvr_aging as
select project_id,event_type, visit_id,count(*) AS mvr_aging, lpd.last_process_date from
sbi_star.d_date a left join(
select distinct project_id, event_type, visit_id, visit_end_Date, dd.d_date_id from sbi_presentation.pd_site_detail psd
join sbi_star.d_date dd
on (to_date(psd.visit_end_date) = dd.iso_date)
where event_type in ('COV','IMV','PSSV','SIV','SMC')
and report_completion_date is NULL
   ) k
   --  on (k.d_date_id = a.d_date_id)
 join (select d.d_date_id as today_id from sbi_star.d_date d where iso_date = CURRENT_DATE()) aa
join sbi_star.sbi_processed_date lpd  

where a.d_date_id between  k.d_date_id and aa.today_id
and a.day_of_week_short_desc not in ('SAT','SUN')
group by project_id,event_type, visit_id,lpd.last_process_date;

