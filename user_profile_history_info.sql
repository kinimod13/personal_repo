#standardSQL
with base AS (
  SELECT
    up.*,
    speciality.name as speciality,
    speciality.speciality_group,
    occupation.name as occupation_name,
    occupation.label as occupation_label,
    so.label as study_objective,
    cast (
       (CASE WHEN up.version = 1
        THEN up.created_at
        ELSE up.updated_at END) as date)                                             AS start_date,
    coalesce(
      date_sub(cast(lead(up.updated_at)
             OVER (
               PARTITION BY up.region, up.user_id
               ORDER BY up.version) as date),
               INTERVAL 1 DAY), current_date()) AS end_date
  FROM `business-intelligence-194510.miamed_bi_test_all_enriched.sf_guard_user_profile_history` up
  JOIN `business-intelligence-194510.miamed_bi_test_all_enriched.sf_guard_user` u
    on u.region = up.region and u.id = up.user_id
  left join `business-intelligence-194510.miamed_bi_test_all_enriched.speciality` as speciality
    on up.region = speciality.region and up.aspired_speciality_id = speciality.id
  left join `business-intelligence-194510.miamed_bi_test_all.occupation` as occupation
    on up.region = occupation.region and up.occupation_id = occupation.id
  left join `business-intelligence-194510.miamed_bi_test_all.study_objective` so on so.region = up.region and so.id = up.study_objective_id
  WHERE
    up.user_id != 125100 AND up.user_id != 107297 and up.user_id != 23560
),
user_profile_history_info as (
  SELECT
    concat(region, '_', cast(user_id as string)) as user_guid,
    region,
    user_id,
    date,
    study_objective,
    stage,
    profession,
    speciality,
    speciality_group,
    occupation_name,
    occupation_label,
    occupation_text
  FROM base, unnest(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date
)
select * from user_profile_history_info