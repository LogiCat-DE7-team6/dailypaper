### sql
main = [
        """
        SELECT MAX(execution_date) FROM slack.main ; 
        """,
        """
        SELECT total
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main) ;
        """,
        """
        SELECT total_diff
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT top_field_domain
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT top_field
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT top_field_count
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """
    ]

field = [
        """
        SELECT physical_sciences
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT life_sciences
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT health_sciences
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT social_sciences
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """,
        """
        SELECT unknown
        FROM slack.main
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.main);
        """
    ]

recommend = [
        """
        SELECT MAX(execution_date) FROM slack.recommendations; 
        """,
        """
        SELECT 
            url
        FROM slack.recommendations
        WHERE type = '인용논문'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """,
        """
        SELECT 
            keyword
        FROM slack.recommendations
        WHERE type = '키워드'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """,
        """
        SELECT 
            url
        FROM slack.recommendations
        WHERE type = '키워드'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """,
        """
        SELECT 
            url
        FROM slack.recommendations
        WHERE type = '하이라이트'
            AND execution_date = (SELECT MAX(execution_date) FROM slack.recommendations);
        """
    ]


type = [
        """
        SELECT article
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT book_chapter
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT "dataset"
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT preprint
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT dissertation
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT book
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT review
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT paratext
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """,
        """
        SELECT others
        FROM slack.type
        WHERE execution_date = (SELECT MAX(execution_date) FROM slack.type);
        """
    ]