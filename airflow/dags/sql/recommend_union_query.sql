-- 1. create if not exist
CREATE TABLE IF NOT EXISTS slack.recommendations (
    execution_date      DATE,
    title               STRING,
    url                 STRING,
    type                STRING,
    keyword             STRING
);

-- 2. delete
DELETE FROM slack.recommendations
WHERE execution_date = '{{ ds }}';

-- 3. insert 
INSERT INTO slack.recommendations (
    execution_date,
    title, 
    url, 
    type, 
    keyword
)
SELECT
    execution_date,
    title,
    openalex_url AS url,
    '하이라이트' AS type,
    Null as keyword
FROM processed.highlight
WHERE fwci > 0
    AND execution_date = '{{ ds }}'

UNION

SELECT 
    execution_date,
    title,
    doi AS url,
    '키워드' AS type,
    keyword
FROM processed.title_keyword
WHERE execution_date = '{{ ds }}'

UNION

SELECT
    execution_date,
    title,
    doi AS url,
    '인용논문' AS type,
    Null as keyword
FROM processed.ref_work
WHERE execution_date = '{{ ds }}'
;