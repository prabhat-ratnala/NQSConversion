drop table if exists sbi_raw.co_status_kpi;
create table sbi_raw.co_status_kpi as
select distinct project_id,
CASE WHEN CO_STATUS IS NULL or CO_STATUS < 60 then 'GREEN'
WHEN CO_STATUS >=60 AND CO_STATUS <= 120 then 'AMBER'
WHEN CO_STATUS >120 then 'RED'
end as kpi_rag,
CO_STATUS as value,
lpd.last_process_date as last_processed_date
FROM (
select project_code as project_id,
datediff(CURRENT_dATE, contract_initiation_date) as co_status
from sbi_presentation.pd_project_contract
where record_type_name = 'Change Order'
and execution_date is NULL
  ) kpi
 join sbi_star.sbi_processed_date lpd;
