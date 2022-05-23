# 北京确诊人数走势
SELECT
    confirmed,
    last_update
FROM covid_19.daily_report
WHERE (country_region = 'China') AND (province_state = 'Beijing')
ORDER BY last_update ASC
LIMIT 1000

# 最新数据
SELECT *
FROM covid_19.daily_report
WHERE CONCAT(combined_key, CAST(last_update, 'String')) IN (
    SELECT CONCAT(combined_key, CAST(last_update, 'String')) AS combined_key
    FROM
    (
        SELECT
            combined_key,
            MAX(last_update) AS last_update
        FROM covid_19.daily_report
        GROUP BY combined_key
    )
)
LIMIT 1

# 最新各国确认数据
SELECT
    country_region,
    SUM(confirmed) AS confirmed
FROM covid_19.daily_report
WHERE CONCAT(combined_key, CAST(last_update, 'String')) IN (
    SELECT CONCAT(combined_key, CAST(last_update, 'String')) AS combined_key
    FROM
    (
        SELECT
            combined_key,
            MAX(last_update) AS last_update
        FROM covid_19.daily_report
        GROUP BY combined_key
    )
)
GROUP BY country_region
LIMIT 1


# 各国确诊病例占比
SELECT
    country_region,
    SUM(confirmed) AS confirmed
FROM covid_19.daily_report
WHERE CONCAT(combined_key, CAST(last_update, 'String')) IN (
    SELECT CONCAT(combined_key, CAST(last_update, 'String')) AS combined_key
    FROM
    (
        SELECT
            combined_key,
            MAX(last_update) AS last_update
        FROM covid_19.daily_report
        GROUP BY combined_key
    )
)
GROUP BY country_region
ORDER BY confirmed DESC
LIMIT 50

# 各国死亡率排行
SELECT
    country_region,
    (deaths / confirmed) * 100 AS deaths_rate
FROM
(
    SELECT
        country_region,
        SUM(confirmed) AS confirmed,
        SUM(deaths) AS deaths
    FROM covid_19.daily_report
    WHERE CONCAT(combined_key, CAST(last_update, 'String')) IN (
        SELECT CONCAT(combined_key, CAST(last_update, 'String')) AS combined_key
        FROM
        (
            SELECT
                combined_key,
                MAX(last_update) AS last_update
            FROM covid_19.daily_report
            GROUP BY combined_key
        )
    )
    GROUP BY country_region
)
WHERE confirmed > 0
ORDER BY deaths_rate DESC
LIMIT 50
