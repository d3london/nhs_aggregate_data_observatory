with source as (
      select * from {{ source('source_tables', 'regpt_2018_10_gp_reg_pat_prac_lsoa_male_oct_18') }}
)
select
    strptime(EXTRACT_DATE, '%d%b%Y')::date as observation_date,
    PRACTICE_CODE as practice_code,
    PRACTICE_NAME as practice_name,
    LSOA_CODE as lsoa_code,
    "Number of Patients" as male_patient_count
from source