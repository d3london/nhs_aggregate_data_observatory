with source as (
      select * from {{ source('source_tables', 'regpt_2020_04_gp_reg_pat_prac_lsoa_female') }}
)
select
    strptime(EXTRACT_DATE, '%d%b%Y')::date as observation_date,
    PRACTICE_CODE as practice_code,
    PRACTICE_NAME as practice_name,
    LSOA_CODE as lsoa_code,
    "Number of Patients" as female_patient_count
from source