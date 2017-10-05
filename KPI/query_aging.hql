drop table if exists sbi_raw.query_aging_kpi;
create table sbi_raw.query_aging_kpi as
select kpi.project_id,
case when query_aging_ratio <= 10 then 'GREEN'
WHEN query_aging_ratio >10 AND query_aging_ratio <= 20 then 'AMBER'
WHEN query_aging_ratio >20 then 'RED'
end as kpi_rag,
query_aging_ratio as value,
lpd.last_process_date as last_processed_date
from (
select pdqa.project_id,
(qagt.qage_gt_30 * 100)/count(query_id) as query_aging_ratio from 
sbi_presentation.pd_dm_query_aging pdqa

left join (select a.project_id, count(a.query_id) as qage_gt_30 from sbi_presentation.pd_dm_query_aging a
           where a.query_age > 30 group by project_id) qagt
on (pdqa.project_id = qagt.project_id)
group by pdqa.project_id,qagt.qage_gt_30
) kpi
join sbi_star.sbi_processed_date lpd;