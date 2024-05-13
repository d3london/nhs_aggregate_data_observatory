select
    observation_date,
    practice_code,
    practice_name,
    lsoa_code,
    male_patient_count,
    female_patient_count,
    total_patient_count
from {{ ref("base__gp_registered_patients_2017_01_male_female")}}
union all
select
    coalesce(m.observation_date, f.observation_date) as observation_date,
    coalesce(m.practice_code, f.practice_code) as practice_code,
    coalesce(m.practice_name, f.practice_name) as practice_name,
    coalesce(m.lsoa_code, f.lsoa_code) as lsoa_code,
    coalesce(m.male_patient_count, '0') as male_patient_count,
    coalesce(f.female_patient_count, '0') as female_patient_count,
    (m.male_patient_count::int + f.female_patient_count::int)::varchar as total_patient_count
from {{ ref("base__gp_registered_patients_2017_04_male")}} as m
full outer join {{ ref("base__gp_registered_patients_2017_04_female")}} as f
on m.practice_code = f.practice_code and m.lsoa_code = f.lsoa_code
union all
select
    coalesce(m.observation_date, f.observation_date) as observation_date,
    coalesce(m.practice_code, f.practice_code) as practice_code,
    coalesce(m.practice_name, f.practice_name) as practice_name,
    coalesce(m.lsoa_code, f.lsoa_code) as lsoa_code,
    coalesce(m.male_patient_count, '0') as male_patient_count,
    coalesce(f.female_patient_count, '0') as female_patient_count,
    (m.male_patient_count::int + f.female_patient_count::int)::varchar as total_patient_count
from {{ ref("base__gp_registered_patients_2017_07_male")}} as m
full outer join {{ ref("base__gp_registered_patients_2017_07_female")}} as f
on m.practice_code = f.practice_code and m.lsoa_code = f.lsoa_code
union all
select
    coalesce(m.observation_date, f.observation_date) as observation_date,
    coalesce(m.practice_code, f.practice_code) as practice_code,
    coalesce(m.practice_name, f.practice_name) as practice_name,
    coalesce(m.lsoa_code, f.lsoa_code) as lsoa_code,
    coalesce(m.male_patient_count, '0') as male_patient_count,
    coalesce(f.female_patient_count, '0') as female_patient_count,
    (m.male_patient_count::int + f.female_patient_count::int)::varchar as total_patient_count
from {{ ref("base__gp_registered_patients_2017_10_male")}} as m
full outer join {{ ref("base__gp_registered_patients_2017_10_female")}} as f
on m.practice_code = f.practice_code and m.lsoa_code = f.lsoa_code