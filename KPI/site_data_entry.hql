drop table if exists sbi_raw.site_data_entry_kpi;
create table sbi_raw.site_data_entry_kpi as
select kpi.project_id,
case when project_median_tde < 8 then 'GREEN'
when project_median_tde >=8 and project_median_tde <=29 then 'AMBER'
when project_median_tde >= 30 then 'RED'
end as kpi_rag,
project_median_tde as value,
lpd.last_process_date as last_processed_date
FROM (
select distinct project_id, project_median_tde from
sbi_presentation.pd_dm_tde) kpi
join sbi_star.sbi_processed_date lpd;