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

 -- get latest updated_at
occupation_get_max as (
select user_id, max(updated_at) max_updated from base
 where
  occupation_name in ( 'doctor t1', 'doctor t2', 'doctor t3', 'doctor t4', 'doctor t5', 'doctor t6' )
  and region = 'eu'
  and profession = 'physician' -- neccessary?
 group by 1
 ),

occupation_detection_base as (
 -- calculate start_date of "assistenzarzt"
 select
  concat ( 'eu_', cast ( base.user_id as string )) as user_guid,
  occupation_name,
  -- get current year number: cast( split ( occupation_name, ' t')[offset(1)] as int64)
  date_sub ( date(max_updated), interval ( cast ( split ( occupation_name, ' t')[offset(1)] as int64) * 12 ) - 12 month)
      as start_date,
  max_updated
 from occupation_get_max max
 join
  base
  on base.updated_at = max.max_updated
  and base.user_id = max.user_id
),

occupation_detection as (
-- with start_date now calculate all 6 assistenz arzt years
 select
  user_guid, 'doctor t1' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'doctor t1' ) as occupation_label,
  start_date,
  date_add ( start_date, interval 12 month ) end_date
 from occupation_detection_base

 union all
 select
  user_guid, 'doctor t2' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'doctor t2' ) as occupation_label,
  date_add ( start_date, interval 12 month) start_date,
  date_add ( start_date, interval 24 month ) end_date
 from occupation_detection_base

 union all
 select
  user_guid, 'doctor t3' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'doctor t3' ) as occupation_label,
  date_add ( start_date, interval 24 month) start_date,
  date_add ( start_date, interval 36 month ) end_date
 from occupation_detection_base

 union all
 select
  user_guid, 'doctor t4' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'doctor t4' ) as occupation_label,
  date_add ( start_date, interval 36 month) start_date,
  date_add ( start_date, interval 48 month ) end_date
 from occupation_detection_base

 union all
 select
  user_guid, 'doctor t5' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'doctor t5' ) as occupation_label,
  date_add ( start_date, interval 48 month) start_date,
  date_add ( start_date, interval 60 month ) end_date
 from occupation_detection_base

 union all
 select
  user_guid, 'doctor t6' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'doctor t6' ) as occupation_label,
  date_add ( start_date, interval 60 month) start_date,
  date_add ( start_date, interval 72 month ) end_date
 from occupation_detection_base

 union all
 select
  user_guid, 'specialist' as occupation_name,
  (select label from `miamed_bi_test_all.occupation` where name = 'specialist' ) as occupation_label,
  date_add ( start_date, interval 72 month) start_date,
  '2030-01-01' as end_date
 from occupation_detection_base
),

-- join via start/end date & use detection if it exists
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
    coalesce( detection.occupation_name, base.occupation_name ) as occupation_name,
    coalesce( detection.occupation_label, base.occupation_label )  as occupation_label,
    occupation_text,
  FROM base, unnest(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date
  LEFT JOIN occupation_detection detection
  ON concat(region, '_', cast(user_id as string)) = detection.user_guid
  AND date between detection.start_date and date_sub ( detection.end_date, interval 1 day )
)
select * from user_profile_history_info