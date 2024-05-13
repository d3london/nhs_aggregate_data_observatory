select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2016_01_male_female")}}
union all
select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2016_04_male_female")}}
union all
select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2016_07_male_female")}}
union all
select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2016_10_male_female")}}