with source as (
      select * from {{ source('source_tables', 'regpt_2016_01_gp_reg_patients_lsoa_alt_tall') }}
)
select
    date '2016-01-01' as observation_date,
    PRACTICE_CODE as practice_code,
    NAME as practice_name,
    LSOA_CODE as lsoa_code,
    "Male Patients" as male_patient_count,
    "Female Patients" as female_patient_count,
    "All Patients" as total_patient_count
from source