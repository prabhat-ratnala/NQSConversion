drop table if exists sbi_raw.investigator_payments_kpi;
create table sbi_raw.investigator_payments_kpi as
select project_number,
case when total_score >= 8 and total_score <= 10 then 'GREEN'
when total_score >= 4 and total_score <= 7 then 'AMBER'
when total_score >= 0 and total_score <= 3 then 'RAW'
END kpi_rag,
total_score as value,
lpd.last_process_date as last_processed_date
from sbi_presentation.pd_ip_scorecards
 join sbi_star.sbi_processed_date lpd;