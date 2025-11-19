-- 1. main 테이블 생성 
CREATE TABLE IF NOT EXISTS slack.main (
    execution_date DATE,
    total BIGINT,
    total_diff BIGINT,
    physical_sciences BIGINT,
    life_sciences BIGINT,
    health_sciences BIGINT,
    social_sciences BIGINT,
    unknown BIGINT,
    top_field_domain STRING,
    top_field STRING,
    top_field_count BIGINT
);

-- 2. main full refresh
DELETE FROM slack.main;

INSERT INTO slack.main (
    execution_date,
    total,
    total_diff,
    physical_sciences,
    life_sciences,
    health_sciences,
    social_sciences,
    unknown,
    top_field_domain,
    top_field,
    top_field_count
)
WITH max_count_field AS (
    SELECT
        execution_date,
        domain,
        field,
        count
    FROM (
        SELECT
            execution_date,
            domain,
            field,
            count,
            ROW_NUMBER() OVER (PARTITION BY execution_date ORDER BY count DESC) AS rn
        FROM raw_data.meta_3days_field
    ) t
    WHERE rn = 1
)
SELECT
    m.execution_date AS execution_date,
    m.total AS total,
    COALESCE(m.total - LAG(m.total) OVER (ORDER BY m.execution_date),0) AS total_diff,
    m.physical_sciences AS physical_sciences,
    m.life_sciences AS life_sciences,
    m.health_sciences AS health_sciences,
    m.social_sciences AS social_sciences,
    m.unknown AS unknown,
    mc.domain AS top_field_domain,
    mc.field AS top_field,
    mc.count AS top_field_count
FROM raw_data.meta_main AS m
LEFT JOIN max_count_field AS mc
    ON m.execution_date = mc.execution_date;