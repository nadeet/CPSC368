"""
build_oracle_database.py

This script is the notebook code from phase3_coba_newest.ipynb, reorganized into a
single runnable Python file for the Phase 3 submission.

Purpose:
- Connect to Oracle
- Drop old tables
- Recreate tables
- Read the cleaned CSV files
- Clean/filter the data exactly as in the notebook
- Insert the records into Oracle

External libraries used beyond oracledb:
- pandas
"""

import oracledb
import pandas as pd


def main():
    # Notebook cells 1-4 combined into one runnable script.
    conn = oracledb.connect(
        user="ora_CWL",
        password="aSTU_ID",
        dsn=oracledb.makedsn("localhost", 1522, service_name="stu")
    )
    cur = conn.cursor()

    # drop old tables first if they already exist
    for table_name in [
        "Letterboxd_Genres",
        "Letterboxd_Studios",
        "Letterboxd_Movies",
        "IMDB_Movies",
        "Rotten_Tomatoes_Movies"
    ]:
        try:
            cur.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
        except oracledb.DatabaseError:
            pass

    cur.execute("""
    CREATE TABLE Letterboxd_Movies (
        id NUMBER PRIMARY KEY,
        name VARCHAR2(200) NOT NULL,
        scaled_rating NUMBER NOT NULL,
        release_year NUMBER NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE Letterboxd_Genres (
        id NUMBER,
        genre_score NUMBER NOT NULL,
        genre VARCHAR2(200) NOT NULL,
        PRIMARY KEY (id, genre),
        FOREIGN KEY (id)
            REFERENCES Letterboxd_Movies(id)
            ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE Letterboxd_Studios (
        id NUMBER,
        studio VARCHAR2(200) NOT NULL,
        PRIMARY KEY (id, studio),
        FOREIGN KEY (id)
            REFERENCES Letterboxd_Movies(id)
            ON DELETE CASCADE
    )
    """)

    cur.execute("""
    CREATE TABLE IMDB_Movies (
        genre_score NUMBER NOT NULL,
        name VARCHAR2(200) NOT NULL,
        scaled_score NUMBER NOT NULL,
        genre VARCHAR2(100),
        year NUMBER NOT NULL,
        company VARCHAR2(200) NOT NULL,
        PRIMARY KEY (name, year)
    )
    """)

    cur.execute("""
    CREATE TABLE Rotten_Tomatoes_Movies (
        movie_title VARCHAR2(200) NOT NULL,
        year NUMBER NOT NULL,
        genre_score NUMBER NOT NULL,
        genre_split VARCHAR2(200),
        scaled_audience_rating NUMBER NOT NULL,
        studio_name VARCHAR2(200) NOT NULL,
        PRIMARY KEY (movie_title, year)
    )
    """)

    conn.commit()

    cur.execute("DELETE FROM Letterboxd_Genres")
    cur.execute("DELETE FROM Letterboxd_Studios")
    cur.execute("DELETE FROM Letterboxd_Movies")
    cur.execute("DELETE FROM IMDB_Movies")
    cur.execute("DELETE FROM Rotten_Tomatoes_Movies")

    conn.commit()

    # read cleaned CSV files
    df_letterboxd_movies = pd.read_csv("selected_letterboxd_movies.csv")
    df_letterboxd_genres = pd.read_csv("selected_letterboxd_genres.csv")
    df_letterboxd_studios = pd.read_csv("studios.csv")
    df_imdb_movies = pd.read_csv("selected_movie_industry.csv")
    df_rotten = pd.read_csv("selected_rotten_tomato_movies.csv")

    # drop NA rows
    df_letterboxd_movies = df_letterboxd_movies.dropna()
    df_letterboxd_genres = df_letterboxd_genres.dropna()
    df_letterboxd_studios = df_letterboxd_studios.dropna()
    df_imdb_movies = df_imdb_movies.dropna()
    df_rotten = df_rotten.dropna()

    # remove duplicates that violate primary keys
    df_letterboxd_studios = df_letterboxd_studios.drop_duplicates(subset=["id", "studio"])
    df_letterboxd_genres = df_letterboxd_genres.drop_duplicates(subset=["id", "genre"])
    df_letterboxd_movies = df_letterboxd_movies.drop_duplicates(subset=["id"])
    df_imdb_movies = df_imdb_movies.drop_duplicates(subset=["name", "year"])
    df_rotten = df_rotten.drop_duplicates(subset=["movie_title", "year"])

    # filter dataset to years 1998–2013
    # This is the same logic as the notebook, with only the Python syntax fixed
    # so it runs as a .py file.
    df_letterboxd_movies = df_letterboxd_movies[
        (df_letterboxd_movies["date"] >= 1998) &
        (df_letterboxd_movies["date"] <= 2013)
    ]

    df_imdb_movies = df_imdb_movies[
        (df_imdb_movies["year"] >= 1998) &
        (df_imdb_movies["year"] <= 2013)
    ]

    df_rotten = df_rotten[
        (df_rotten["year"] >= 1998) &
        (df_rotten["year"] <= 2013)
    ]

    # keep only genres/studios whose movie id exists
    valid_movie_ids = set(df_letterboxd_movies["id"])

    df_letterboxd_genres = df_letterboxd_genres[
        df_letterboxd_genres["id"].isin(valid_movie_ids)
    ]

    df_letterboxd_studios = df_letterboxd_studios[
        df_letterboxd_studios["id"].isin(valid_movie_ids)
    ]

    # Letterboxd Movies
    movies_data = [
        (
            int(r["id"]),
            str(r["name"]),
            float(r["scaled_rating"]),
            int(r["date"])
        )
        for _, r in df_letterboxd_movies.iterrows()
    ]

    cur.executemany("""
    INSERT INTO Letterboxd_Movies (id, name, scaled_rating, release_year)
    VALUES (:1, :2, :3, :4)
    """, movies_data)

    # Letterboxd Genres
    genres_data = [
        (
            int(r["id"]),
            float(r["genre_score"]),
            str(r["genre"])
        )
        for _, r in df_letterboxd_genres.iterrows()
    ]

    cur.executemany("""
    INSERT INTO Letterboxd_Genres (id, genre_score, genre)
    VALUES (:1, :2, :3)
    """, genres_data)

    # Letterboxd Studios
    studios_data = [
        (
            int(r["id"]),
            str(r["studio"])
        )
        for _, r in df_letterboxd_studios.iterrows()
    ]

    cur.executemany("""
    INSERT INTO Letterboxd_Studios (id, studio)
    VALUES (:1, :2)
    """, studios_data)

    # IMDB Movies
    imdb_data = [
        (
            float(r["genre_score"]),
            str(r["name"]),
            float(r["scaled_score"]),
            str(r["genre"]),
            int(r["year"]),
            str(r["company"])
        )
        for _, r in df_imdb_movies.iterrows()
    ]

    cur.executemany("""
    INSERT INTO IMDB_Movies (genre_score, name, scaled_score, genre, year, company)
    VALUES (:1, :2, :3, :4, :5, :6)
    """, imdb_data)

    # Rotten Tomatoes
    rotten_data = [
        (
            str(r["movie_title"]),
            int(r["year"]),
            float(r["genre_score"]),
            str(r["genre_split"]),
            float(r["scaled_audience_rating"]),
            str(r["studio_name"])
        )
        for _, r in df_rotten.iterrows()
    ]

    cur.executemany("""
    INSERT INTO Rotten_Tomatoes_Movies
    (movie_title, year, genre_score, genre_split, scaled_audience_rating, studio_name)
    VALUES (:1, :2, :3, :4, :5, :6)
    """, rotten_data)

    conn.commit()
    print("Database tables created and data inserted successfully.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
