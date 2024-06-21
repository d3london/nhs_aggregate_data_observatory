{{ config(materialized="table") }}

select
    observation_date,
    lsoa_code,
    sum(male_patient_count) as total_male,
    {{ calculate_percentage('sum(male_patient_count)', 'sum(total_patient_count)') }} as pct_male,
    sum(female_patient_count) as total_female,
    {{ calculate_percentage('sum(female_patient_count)', 'sum(total_patient_count)') }} as pct_female,    
    sum(total_patient_count) as total_patients
from {{ ref("int__gp_registered_patients") }}
group by observation_date, lsoa_code