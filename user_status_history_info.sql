#standardSQL
with semester_starts as (
  select
    cast(date as date) as semester_start
  from unnest(GENERATE_DATE_ARRAY(cast('2000-04-20' as date), cast('2030-10-20' as date), INTERVAL 6 MONTH)) date
),
semester as (
  SELECT
    semester_start,
    lead(date_sub(semester_start, INTERVAL 1 day)) over (order by semester_start) as semester_end,
    ROW_NUMBER() OVER() AS global_semester
  FROM semester_starts
),
base AS (
  SELECT
    region,
    user_id,
    updated_at,
    exam_id,
    preclinical_exam_id,
    profession,
    graduation_year,
    profession_country_id as country_id,
    version,
    created_at,
    coalesce((lag(coalesce(exam_id, -1))
              OVER (
                PARTITION BY region, user_id
                ORDER BY version) = coalesce(exam_id, -1))
             AND (lag(coalesce(preclinical_exam_id, -1))
                  OVER (
                    PARTITION BY region, user_id
                    ORDER BY version) = coalesce(preclinical_exam_id, -1))
             AND (lag(coalesce(graduation_year, -1))
                  OVER (
                    PARTITION BY region, user_id
                    ORDER BY version) = coalesce(graduation_year, -1))
             AND (lag(coalesce(profession, ''))
                  OVER (
                    PARTITION BY region, user_id
                    ORDER BY version) = coalesce(profession, ''))
             AND (lag(coalesce(profession_country_id, ''))
                  OVER (
                    PARTITION BY region, user_id
                    ORDER BY version) = coalesce(profession_country_id, '')),
             FALSE) AS duplicate
  FROM `business-intelligence-194510.miamed_bi_test_all.sf_guard_user_profile_history`
),
base_2 AS (
  SELECT
    CASE
      WHEN version = 1
      THEN cast(created_at as DATE)
      ELSE cast(updated_at as DATE) END                                                AS start_date,
    coalesce(lead(date_sub(cast(updated_at as date), INTERVAL 1 DAY))
             OVER (
               PARTITION BY region, user_id
               ORDER BY version), current_date()) AS end_date,
    exam_id                                                                    AS exam_id,
    preclinical_exam_id                                                        AS preclinical_exam_id,
    region,
    user_id,
    graduation_year,
    country_id,
    profession
  FROM base
  WHERE NOT duplicate --AND (exam_id NOTNULL OR preclinical_exam_id NOTNULL OR profession NOTNULL)
),
base_3 AS (
  SELECT
    b.region,
    b.user_id,
    profession,
    graduation_year,
    country_id,
    start_date,
    end_date,
    CASE WHEN preclinical_exam_id IS NOT NULL
      THEN date_add(cast(concat(e.year, '-', e.month, '-', coalesce(e.day, '15')) as date), INTERVAL 3 YEAR)
    WHEN exam_id IS NOT NULL
      THEN cast(concat(e.year, '-', e.month, '-', coalesce(e.day, '15')) as date)
    END                                AS exam_date
  FROM base_2 b
  LEFT JOIN `business-intelligence-194510.miamed_bi_test_all.exam` e ON
    b.region = e.region and coalesce(b.preclinical_exam_id, b.exam_id) = e.id
),
base_3_5 as (
  select
    b3.*,
    array(
      select struct(semester_start, semester_end, global_semester) from semester s where
       (s.semester_start BETWEEN b3.start_date AND b3.end_date)
        OR (s.semester_end BETWEEN b3.start_date AND b3.end_date)
        OR (b3.start_date BETWEEN s.semester_start AND s.semester_end)
        OR (b3.end_date BETWEEN s.semester_start AND s.semester_end)
    ) as semesters,
    array(
      select struct(semester_start, semester_end, global_semester) from semester s where
       b3.exam_date >= s.semester_start AND b3.exam_date < s.semester_end
    ) as user_semesters
  from base_3 b3
),
base_4 as (
  select
    b35.region,
    b35.user_id,
    date,
    b35.profession,
    b35.country_id,
    b35.graduation_year,
    b35.start_date,
    b35.end_date,
    s.semester_start,
    s.semester_end,
    10 - (us.global_semester - s.global_semester) as user_semester
  from base_3_5 b35
  left join unnest(semesters) as s
  left join unnest(user_semesters) as us
  cross join unnest(GENERATE_DATE_ARRAY(greatest(s.semester_start, b35.start_date), least(s.semester_end, b35.end_date), INTERVAL 1 DAY)) date
),
status_history as (
  SELECT
    concat(region, '_', cast(user_id as string)) as user_guid,
    region,
    user_id,
    date,
    case when region = 'eu' then (
      CASE WHEN user_semester <= 0
        THEN 'pre_cycle'
      WHEN user_semester <= 3
        THEN 'VK'
      WHEN user_semester <= 4
        THEN 'M1'
      WHEN user_semester <= 9
        THEN 'K'
      WHEN user_semester <= 10
        THEN 'M2'
      WHEN user_semester <= 13
        THEN 'PJ-M3-A05'
      WHEN user_semester >= 14 AND (profession != 'physician' or profession is null)
        THEN 'A'
      WHEN (user_semester is null or user_semester >= 14) AND profession = 'physician'
        THEN 'A-label'
      ELSE 'Unknown DE'
      END)
    when region = 'us' and country_id in ('US') then (
      CASE WHEN graduation_year > extract(year from date_add((date), INTERVAL 43 month))
        THEN 'pre_cycle'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 43 month))
        THEN '1st year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 31 month))
        THEN '2nd year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 19 month))
        THEN '3rd year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 7 month))
        THEN '4th year'
      WHEN graduation_year < extract(year from date_add(date, INTERVAL 7 month))
        THEN 'physician'
      WHEN graduation_year is null AND profession = 'physician'
        THEN 'physician label'
      ELSE 'Unknown US'
      END)
       when region = 'us' and country_id in ('AG',	'AI',	'AW',	'BB',	'BQ','BZ','CW','KY','DM',	'GD','JM','KN',	'LC',	'MS',	'PR','SX','TT',	'VC') then (
      CASE WHEN graduation_year > extract(year from date_add((date), INTERVAL 55 month))
        THEN 'pre_cycle'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 55 month))
        THEN '1st year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 43 month))
        THEN '2nd year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 31 month))
        THEN '3rd year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 19 month))
        THEN '4th year'
       WHEN graduation_year = extract(year from date_add(date, INTERVAL 7 month))
        THEN '5th year'
      WHEN graduation_year < extract(year from date_add(date, INTERVAL 7 month))
        THEN 'physician'
      WHEN graduation_year is null AND profession = 'physician'
        THEN 'physician label'
      ELSE 'Unknown CB'
      END)
        when region = 'us' and country_id not in (
'US','AG',	'AI',	'AW',	'BB',	'BQ','BZ','CW','KY','DM',	'GD','JM','KN',	'LC',	'MS',	'PR','SX','TT',	'VC'
) then (
      CASE WHEN graduation_year > extract(year from date_add(date, INTERVAL 66 month))
        THEN 'pre_cycle'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 66 month))
        THEN '1st year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 54 month))
        THEN '2nd year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 42 month))
        THEN '3rd year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 30 month))
        THEN '4th year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 18 month))
        THEN '5th year'
      WHEN graduation_year = extract(year from date_add(date, INTERVAL 6 month))
        THEN '6th year'
      WHEN graduation_year < extract(year from date_add(date, INTERVAL 6 month))
        THEN 'physician'
      WHEN graduation_year is null AND profession = 'physician'
        THEN 'physician label'
      ELSE 'Unknown INT'
      END)
    else 'Unknown'
    end AS status,
    user_semester
  FROM base_4
),
graduation_de as (
  SELECT
    user_guid,
    extract( year from min(date) ) as year
  FROM status_history
  WHERE
    region = 'eu'
    AND status = 'A'
  GROUP BY
    user_guid
)
SELECT
 status_history.*,
 graduation_de.year as graduation_de
FROM status_history
LEFT JOIN
 graduation_de
 ON graduation_de.user_guid = status_history.user_guid