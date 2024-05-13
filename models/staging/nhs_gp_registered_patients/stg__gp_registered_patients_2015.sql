select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2015_01_male_female")}}
union all
select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2015_04_male_female")}}
union all
select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2015_07_male_female")}}
union all
select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2015_10_male_female")}}