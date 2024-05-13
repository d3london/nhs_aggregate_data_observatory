with source as (
      select * from {{ source('source_tables', 'regpt_2014_04_gp_reg_patients_04_2014_totals_lsoa_alt') }}
)
select
    date '2014-04-01' as observation_date,
    PRACTICE_CODE as practice_code,
    ORG_NAME as practice_name,
    LSOA_CODE as lsoa_code,
    "Male Patients" as male_patient_count,
    "Female Patients" as female_patient_count,
    "All Patients" as total_patient_count
from source