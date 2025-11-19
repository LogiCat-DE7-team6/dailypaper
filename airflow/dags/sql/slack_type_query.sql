-- 1. type 테이블 생성
CREATE TABLE IF NOT EXISTS slack.type (
    execution_date DATE,
    article BIGINT,
    book_chapter BIGINT,
    "dataset" BIGINT,
    preprint BIGINT,
    dissertation BIGINT,
    book BIGINT,
    review BIGINT,
    paratext BIGINT,
    others BIGINT
);

-- 2. type full refresh
DELETE FROM slack.type;

INSERT INTO slack.type (
    execution_date,
    article,
    book_chapter,
    "dataset",
    preprint,
    dissertation,
    book,
    review,
    paratext,
    others
)
SELECT
    execution_date,
    article,
    book_chapter,
    dataset,
    preprint,
    dissertation,
    book,
    review,
    paratext,
    COALESCE(other,0)+ COALESCE(libguides,0)+ COALESCE(letter,0)+ COALESCE(reference_entry,0)+ COALESCE(report,0)
    + COALESCE(peer_review,0)+ COALESCE(editorial,0)+ COALESCE(erratum,0)+ COALESCE(standard,0)+ COALESCE("grant",0)
    + COALESCE(supplementary_materials,0)+ COALESCE(retraction,0)+ COALESCE(book_section,0)+ COALESCE("database",0)
    + COALESCE(software,0)+ COALESCE(report_component,0) AS others
FROM raw_data.meta_type;