"""
run_oracle_analysis.py

This script is the notebook query code from phase3_coba_newest.ipynb, reorganized
into a single runnable Python file for the Phase 3 submission.

Purpose:
- Connect to Oracle
- Execute SQL queries
- Retrieve and process results into pandas DataFrames
- Reproduce the final tables from the notebook
- Save the query results to CSV files for easy reuse in the final paper

External libraries used beyond oracledb:
- pandas
"""

import oracledb
import pandas as pd


def run_query(cur, sql):
    cur.execute(sql)
    cols = [c[0] for c in cur.description]
    rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def main():
    # Notebook cells 1, 2, 5-13 converted into a standalone analysis script.
    conn = oracledb.connect(
        user="ora_CWL",
        password="aSTU_ID",
        dsn=oracledb.makedsn("localhost", 1522, service_name="stu")
    )
    cur = conn.cursor()

    # Validation counts from notebook cells 6-10
    print("\nLetterboxd_Movies count")
    print(run_query(cur, "SELECT COUNT(*) FROM Letterboxd_Movies").to_string(index=False))

    print("\nLetterboxd_Genres count")
    print(run_query(cur, "SELECT COUNT(*) FROM Letterboxd_Genres").to_string(index=False))

    print("\nLetterboxd_Studios count")
    print(run_query(cur, "SELECT COUNT(*) FROM Letterboxd_Studios").to_string(index=False))

    print("\nIMDB_Movies count")
    print(run_query(cur, "SELECT COUNT(*) FROM IMDB_Movies").to_string(index=False))

    print("\nRotten_Tomatoes_Movies count")
    print(run_query(cur, "SELECT COUNT(*) FROM Rotten_Tomatoes_Movies").to_string(index=False))

    df_a = run_query(cur, """
    SELECT mi.year AS year_interval,
            AVG(mi.scaled_score) AS avg_imdb_rating,
            AVG(lm.scaled_rating) AS avg_letterboxd_rating,
            AVG(rt.scaled_audience_rating) AS avg_rotten_rating
    FROM IMDB_Movies mi
    JOIN Rotten_Tomatoes_Movies rt
        ON mi.name = rt.movie_title
       AND mi.year = rt.year
    JOIN Letterboxd_Movies lm
        ON mi.name = lm.name
       AND mi.year = lm.release_year
    WHERE mi.year BETWEEN 1998 AND 2013
    GROUP BY mi.year
    ORDER BY year_interval
    """)

    print("\nQuery A results")
    print(df_a.to_string(index=False))

    df_b = run_query(cur, """
    SELECT genre_category,
           AVG(lm.scaled_rating) AS avg_audience_rating,
           COUNT(*) AS total_films
    FROM (
        SELECT lg.id,
               'emotionally_positive' AS genre_category
        FROM LETTERBOXD_GENRES lg
        GROUP BY lg.id
        HAVING AVG(lg.genre_score) > 0

        UNION ALL

        SELECT lg.id,
               'fear_based' AS genre_category
        FROM LETTERBOXD_GENRES lg
        GROUP BY lg.id
        HAVING AVG(lg.genre_score) < 0
    ) categorized_films
    JOIN LETTERBOXD_MOVIES lm
        ON categorized_films.id = lm.id
    GROUP BY genre_category
    """)

    print("\nQuery B results")
    print(df_b.to_string(index=False))

    df_c = run_query(cur, """
    SELECT 'major' AS studio_group,
           avg(lm.scaled_rating) AS avg_rating,
           stddev(lm.scaled_rating) AS rating_variability,
           count(DISTINCT lm.id) AS total_films
    FROM Letterboxd_Movies lm
    JOIN Letterboxd_Studios ls
        ON lm.id = ls.id
    WHERE ls.studio IN (
        SELECT studio
        FROM Letterboxd_Studios
        GROUP BY studio
        HAVING count(DISTINCT id) >= 20
    )

    UNION

    SELECT 'small' AS studio_group,
           avg(lm.scaled_rating) AS avg_rating,
           stddev(lm.scaled_rating) AS rating_variability,
           count(DISTINCT lm.id) AS total_films
    FROM Letterboxd_Movies lm
    JOIN Letterboxd_Studios ls
        ON lm.id = ls.id
    WHERE ls.studio IN (
        SELECT studio
        FROM Letterboxd_Studios
        GROUP BY studio
        HAVING count(DISTINCT id) < 20
    )
    """)

    print("\nQuery C results")
    print(df_c.to_string(index=False))

    # Save the final paper tables so the exact results can be reproduced outside Jupyter.
    df_a.to_csv("query_a_results.csv", index=False)
    df_b.to_csv("query_b_results.csv", index=False)
    df_c.to_csv("query_c_results.csv", index=False)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
