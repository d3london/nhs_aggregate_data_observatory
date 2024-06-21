{% set stage_models = [
    ref("stg__gp_registered_patients_2014"),
    ref("stg__gp_registered_patients_2015"),
    ref("stg__gp_registered_patients_2016"),
    ref("stg__gp_registered_patients_2017"),
    ref("stg__gp_registered_patients_2018"),
    ref("stg__gp_registered_patients_2019"),
    ref("stg__gp_registered_patients_2020")
] %}

{% for model in stage_models %}
    select
        observation_date::date as observation_date,
        practice_code,
        practice_name,
        lsoa_code,
        male_patient_count::int as male_patient_count,
        female_patient_count::int as female_patient_count,
        total_patient_count::int as total_patient_count
    from {{ model }} {{ "union all" if not loop.last }}
{% endfor %}