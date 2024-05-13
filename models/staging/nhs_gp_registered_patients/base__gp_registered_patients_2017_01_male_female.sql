with source as (
      select * from {{ source('source_tables', 'regpt_2017_01_gp_reg_patients_lsoa_alt_tall') }}
)
select
    date '2017-01-01' as observation_date,
    PRACTICE_CODE as practice_code,
    NAME as practice_name,
    LSOA_CODE as lsoa_code,
    MALE_PATIENTS as male_patient_count,
    FEMALE_PATIENTS as female_patient_count,
    ALL_PATIENTS as total_patient_count
from source