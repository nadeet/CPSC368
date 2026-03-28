"""
load_to_mongodb_matching_oracle.py

Loads the cleaned Phase 4 CSV files into MongoDB using a film-centered
document structure.

Expected files:
    data/selected_imdb_movies.csv
    data/selected_letterboxd_genres.csv
    data/selected_letterboxd_movies.csv
    data/selected_rotten_tomato_movies.csv
    data/studios.csv

Install dependencies:
    pip install pandas pymongo

Run:
    python python_deliverables/load_to_mongodb_matching_oracle.py
"""

from pathlib import Path
from collections import defaultdict
import math
import re

import pandas as pd
from pymongo import MongoClient

# 1. CONFIGURATION

DATA_DIR = Path("data")

LETTERBOXD_MOVIES_FILE = DATA_DIR / "selected_letterboxd_movies.csv"
LETTERBOXD_GENRES_FILE = DATA_DIR / "selected_letterboxd_genres.csv"
IMDB_FILE = DATA_DIR / "selected_imdb_movies.csv"
ROTTEN_TOMATOES_FILE = DATA_DIR / "selected_rotten_tomato_movies.csv"
STUDIOS_FILE = DATA_DIR / "studios.csv"

# Replace mongo_username and mongo_password with your UBC MongoDB credentials.
mongo_username = "mongo_username" # UBC CWL Username
mongo_password = "mongo_password" # "a" + UBC Student Number

MONGO_URI = f"mongodb://{mongo_username}:{mongo_password}@localhost:27017/{mongo_username}"
DATABASE_NAME = mongo_username
COLLECTION_NAME = "films"

# If True, the collection is dropped and rebuilt each time.
DROP_COLLECTION_FIRST = True


# 2. HELPER FUNCTIONS

def normalize_title(title):
    """
    Normalize a movie title so that title + year can be used as a matching key
    across different CSV files.
    """
    if pd.isna(title):
        return ""

    title = str(title).strip().lower()
    title = title.replace("&", "and")
    title = re.sub(r"[^a-z0-9\s]", "", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def make_match_key(title, year):
    """
    Create a stable key like:
        "the_shining_1980"
    except spaces are kept normalized rather than replaced.
    """
    if pd.isna(title) or pd.isna(year):
        return None

    try:
        year_int = int(float(year))
    except (TypeError, ValueError):
        return None

    normalized = normalize_title(title)
    if not normalized:
        return None

    return f"{normalized}_{year_int}"


def to_int(value):
    if pd.isna(value):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_float(value):
    if pd.isna(value):
        return None
    try:
        number = float(value)
        if math.isnan(number):
            return None
        return number
    except (TypeError, ValueError):
        return None


def to_str(value):
    if pd.isna(value):
        return None
    value = str(value).strip()
    return value if value else None


def unique_list_of_dicts(items):
    """
    Remove duplicate dictionaries while preserving order.
    """
    seen = set()
    result = []

    for item in items:
        key = tuple(sorted(item.items()))
        if key not in seen:
            seen.add(key)
            result.append(item)

    return result


def unique_strings(items):
    """
    Remove duplicate strings while preserving order.
    """
    seen = set()
    result = []

    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)

    return result


def compute_genre_summary(letterboxd_genres):
    """
    Compute the average Letterboxd genre score for a movie and assign a category.
    """
    scores = [
        genre["genre_score"]
        for genre in letterboxd_genres
        if genre.get("genre_score") is not None
    ]

    if not scores:
        return {
            "avg_genre_score": None,
            "category": "neutral"
        }

    avg_score = sum(scores) / len(scores)

    if avg_score > 0:
        category = "emotionally_positive"
    elif avg_score < 0:
        category = "fear_based"
    else:
        category = "neutral"

    return {
        "avg_genre_score": round(avg_score, 4),
        "category": category
    }

# 3. LOAD CSV FILES

def load_data():
    lb_movies = pd.read_csv(LETTERBOXD_MOVIES_FILE)
    lb_genres = pd.read_csv(LETTERBOXD_GENRES_FILE)
    imdb_movies = pd.read_csv(IMDB_FILE)
    rt_movies = pd.read_csv(ROTTEN_TOMATOES_FILE)
    studios = pd.read_csv(STUDIOS_FILE)

    # Standardize column names
    for df in [lb_movies, lb_genres, imdb_movies, rt_movies, studios]:
        df.columns = [col.strip().lower() for col in df.columns]

    # drop NA
    lb_movies = lb_movies.dropna()
    lb_genres = lb_genres.dropna()
    imdb_movies = imdb_movies.dropna()
    rt_movies = rt_movies.dropna()
    studios = studios.dropna()

    # remove duplicates
    lb_movies = lb_movies.drop_duplicates(subset=["id"])
    lb_genres = lb_genres.drop_duplicates(subset=["id", "genre"])
    studios = studios.drop_duplicates(subset=["id", "studio"])
    imdb_movies = imdb_movies.drop_duplicates(subset=["name", "year"])
    rt_movies = rt_movies.drop_duplicates(subset=["movie_title", "year"])

    lb_movies = lb_movies[
        (lb_movies["date"] >= 1998) &
        (lb_movies["date"] <= 2013)
    ]

    imdb_movies = imdb_movies[
        (imdb_movies["year"] >= 1998) &
        (imdb_movies["year"] <= 2013)
    ]

    rt_movies = rt_movies[
        (rt_movies["year"] >= 1998) &
        (rt_movies["year"] <= 2013)
    ]
    
    valid_ids = set(lb_movies["id"])

    lb_genres = lb_genres[lb_genres["id"].isin(valid_ids)]
    studios = studios[studios["id"].isin(valid_ids)]

    return lb_movies, lb_genres, imdb_movies, rt_movies, studios

# 4. BUILD LOOKUPS FOR EMBEDDED DATA

def build_letterboxd_genres_lookup(lb_genres):
    """
    Build:
        { letterboxd_id: [ {name: ..., genre_score: ...}, ... ] }
    """
    genres_by_id = defaultdict(list)

    cleaned = lb_genres.dropna(subset=["id", "genre"]).drop_duplicates()

    for movie_id, genre_score, genre in cleaned[["id", "genre_score", "genre"]].itertuples(index=False, name=None):
        movie_id = to_int(movie_id)
        genre_name = to_str(genre)
        genre_score = to_int(genre_score)

        if movie_id is None or genre_name is None:
            continue

        genres_by_id[movie_id].append({
            "name": genre_name,
            "genre_score": genre_score
        })

    for movie_id in genres_by_id:
        genres_by_id[movie_id] = unique_list_of_dicts(genres_by_id[movie_id])

    return genres_by_id


def build_studios_lookup(studios_df):
    """
    Build:
        { letterboxd_id: [studio1, studio2, ...] }
    """
    studios_by_id = defaultdict(list)

    cleaned = studios_df.dropna(subset=["id", "studio"]).drop_duplicates()

    for movie_id, studio in cleaned[["id", "studio"]].itertuples(index=False, name=None):
        movie_id = to_int(movie_id)
        studio_name = to_str(studio)

        if movie_id is None or studio_name is None:
            continue

        studios_by_id[movie_id].append(studio_name)

    for movie_id in studios_by_id:
        studios_by_id[movie_id] = unique_strings(studios_by_id[movie_id])

    return studios_by_id


def build_imdb_movies_lookup(imdb_movies):
    """
    Build:
        { match_key: {scaled_score, genre, company, title, release_year} }

    selected_imdb_movies.csv already appears to have one row per title+year,
    so no grouping is needed.
    """
    lookup = {}

    imdb_movies = imdb_movies.copy()
    imdb_movies["match_key"] = imdb_movies.apply(
        lambda row: make_match_key(row["name"], row["year"]),
        axis=1
    )

    for row in imdb_movies.itertuples(index=False):
        key = getattr(row, "match_key")
        if not key:
            continue

        lookup[key] = {
            "title": to_str(getattr(row, "name")),
            "release_year": to_int(getattr(row, "year")),
            "scaled_score": to_float(getattr(row, "scaled_score")),
            "genre": {
                "name": to_str(getattr(row, "genre")),
                "genre_score": to_int(getattr(row, "genre_score"))
            } if to_str(getattr(row, "genre")) is not None else None,
            "company": to_str(getattr(row, "company"))
        }

    return lookup


def build_rotten_tomatoes_lookup(rt_movies):
    """
    Build:
        { match_key: {scaled_audience_rating, studio_name, genres, title, release_year} }

    Rotten Tomatoes has multiple rows per movie because a movie can appear
    with multiple genre_split values, so we group by normalized title + year.
    """
    lookup = {}

    rt_movies = rt_movies.copy()
    rt_movies["match_key"] = rt_movies.apply(
        lambda row: make_match_key(row["movie_title"], row["year"]),
        axis=1
    )

    grouped = rt_movies.dropna(subset=["match_key"]).groupby("match_key", sort=False)

    for key, group in grouped:
        first_row = group.iloc[0]

        rt_genres = []
        genre_rows = group[["genre_split", "genre_score"]].dropna(subset=["genre_split"]).drop_duplicates()

        for genre_name, genre_score in genre_rows.itertuples(index=False, name=None):
            rt_genres.append({
                "name": to_str(genre_name),
                "genre_score": to_int(genre_score)
            })

        lookup[key] = {
            "title": to_str(first_row["movie_title"]),
            "release_year": to_int(first_row["year"]),
            "scaled_audience_rating": to_float(first_row["scaled_audience_rating"]),
            "studio_name": to_str(first_row["studio_name"]),
            "genres": unique_list_of_dicts(rt_genres)
        }

    return lookup

# 5. BUILD FINAL FILM DOCUMENTS

def build_documents(lb_movies, genres_by_id, studios_by_id, imdb_movies_lookup, rt_lookup):
    """
    Build MongoDB documents.

    Strategy:
    1. Use every Letterboxd movie as a base document.
    2. Attach imdb_movies and Rotten Tomatoes data using title + year matching.
    3. Add extra documents for imdb_movies / Rotten Tomatoes titles that
       do not exist in Letterboxd, so no cleaned source data is lost.
    """
    documents = []
    matched_keys = set()

    lb_movies = lb_movies.copy()
    lb_movies["release_year"] = pd.to_numeric(lb_movies["date"], errors="coerce").astype("Int64")
    lb_movies["match_key"] = lb_movies.apply(
        lambda row: make_match_key(row["name"], row["release_year"]),
        axis=1
    )

    # A. Build documents for all Letterboxd movies
    for row in lb_movies.itertuples(index=False):
        letterboxd_id = to_int(getattr(row, "id"))
        match_key = getattr(row, "match_key")

        lb_genres = genres_by_id.get(letterboxd_id, [])
        lb_studios = studios_by_id.get(letterboxd_id, [])

        imdb_movies_doc = imdb_movies_lookup.get(match_key)
        rt_doc = rt_lookup.get(match_key)

        doc = {
            "letterboxd_id": letterboxd_id,
            "title": to_str(getattr(row, "name")),
            "release_year": to_int(getattr(row, "release_year")),
            "match_key": match_key,

            "ratings": {
                "letterboxd": {
                    "scaled_rating": to_float(getattr(row, "scaled_rating"))
                },
                "imdb_movies": {
                    "scaled_score": imdb_movies_doc["scaled_score"]
                } if imdb_movies_doc else None,
                "rotten_tomatoes": {
                    "scaled_audience_rating": rt_doc["scaled_audience_rating"]
                } if rt_doc else None
            },

            "genres": {
                "letterboxd": lb_genres,
                "imdb_movies": imdb_movies_doc["genre"] if imdb_movies_doc else None,
                "rotten_tomatoes": rt_doc["genres"] if rt_doc else []
            },

            "genre_summary": compute_genre_summary(lb_genres),

            "studios": lb_studios,

            "source_metadata": {
                "imdb_movies_company": imdb_movies_doc["company"] if imdb_movies_doc else None,
                "rotten_tomatoes_studio_name": rt_doc["studio_name"] if rt_doc else None
            },

            "source_flags": {
                "has_letterboxd": True,
                "has_imdb_movies": imdb_movies_doc is not None,
                "has_rotten_tomatoes": rt_doc is not None
            }
        }

        documents.append(doc)

        if match_key:
            matched_keys.add(match_key)

    # B. Add extra docs for movies that exist only in imdb_movies / RT
    external_only_docs = {}

    for key, imdb_movies_doc in imdb_movies_lookup.items():
        if key in matched_keys:
            continue

        external_only_docs[key] = {
            "letterboxd_id": None,
            "title": imdb_movies_doc["title"],
            "release_year": imdb_movies_doc["release_year"],
            "match_key": key,

            "ratings": {
                "letterboxd": None,
                "imdb_movies": {
                    "scaled_score": imdb_movies_doc["scaled_score"]
                },
                "rotten_tomatoes": None
            },

            "genres": {
                "letterboxd": [],
                "imdb_movies": imdb_movies_doc["genre"],
                "rotten_tomatoes": []
            },

            "genre_summary": {
                "avg_genre_score": None,
                "category": "neutral"
            },

            "studios": [],

            "source_metadata": {
                "imdb_movies_company": imdb_movies_doc["company"],
                "rotten_tomatoes_studio_name": None
            },

            "source_flags": {
                "has_letterboxd": False,
                "has_imdb_movies": True,
                "has_rotten_tomatoes": False
            }
        }

    for key, rt_doc in rt_lookup.items():
        if key in matched_keys:
            continue

        if key not in external_only_docs:
            external_only_docs[key] = {
                "letterboxd_id": None,
                "title": rt_doc["title"],
                "release_year": rt_doc["release_year"],
                "match_key": key,

                "ratings": {
                    "letterboxd": None,
                    "imdb_movies": None,
                    "rotten_tomatoes": {
                        "scaled_audience_rating": rt_doc["scaled_audience_rating"]
                    }
                },

                "genres": {
                    "letterboxd": [],
                    "imdb_movies": None,
                    "rotten_tomatoes": rt_doc["genres"]
                },

                "genre_summary": {
                    "avg_genre_score": None,
                    "category": "neutral"
                },

                "studios": [],

                "source_metadata": {
                    "imdb_movies_company": None,
                    "rotten_tomatoes_studio_name": rt_doc["studio_name"]
                },

                "source_flags": {
                    "has_letterboxd": False,
                    "has_imdb_movies": False,
                    "has_rotten_tomatoes": True
                }
            }
        else:
            # Merge RT data into an already-created external-only document
            external_only_docs[key]["ratings"]["rotten_tomatoes"] = {
                "scaled_audience_rating": rt_doc["scaled_audience_rating"]
            }
            external_only_docs[key]["genres"]["rotten_tomatoes"] = rt_doc["genres"]
            external_only_docs[key]["source_metadata"]["rotten_tomatoes_studio_name"] = rt_doc["studio_name"]
            external_only_docs[key]["source_flags"]["has_rotten_tomatoes"] = True

    documents.extend(external_only_docs.values())
    return documents

# 6. LOAD INTO MONGODB

def load_into_mongodb(documents):
    """
    Connect to MongoDB, optionally drop the collection, insert documents,
    and create indexes.
    """
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    if DROP_COLLECTION_FIRST:
        collection.drop()

    if documents:
        collection.insert_many(documents, ordered=False)

    # Helpful indexes
    collection.create_index("letterboxd_id")
    collection.create_index("match_key")
    collection.create_index("release_year")
    collection.create_index("genre_summary.category")
    collection.create_index("studios")
    collection.create_index("source_flags.has_letterboxd")
    collection.create_index("source_flags.has_imdb_movies")
    collection.create_index("source_flags.has_rotten_tomatoes")

    print(f"Inserted {len(documents)} documents into {DATABASE_NAME}.{COLLECTION_NAME}")
    print("Collection count:", collection.count_documents({}))
    print("With Letterboxd:", collection.count_documents({"source_flags.has_letterboxd": True}))
    print("With IMDB:", collection.count_documents({"source_flags.has_imdb_movies": True}))
    print("With Rotten Tomatoes:", collection.count_documents({"source_flags.has_rotten_tomatoes": True}))

    client.close()

# 7. MAIN

def main():
    lb_movies, lb_genres, imdb_movies, rt_movies, studios_df = load_data()

    genres_by_id = build_letterboxd_genres_lookup(lb_genres)
    studios_by_id = build_studios_lookup(studios_df)
    imdb_movies_lookup = build_imdb_movies_lookup(imdb_movies)
    rt_lookup = build_rotten_tomatoes_lookup(rt_movies)

    documents = build_documents(
        lb_movies=lb_movies,
        genres_by_id=genres_by_id,
        studios_by_id=studios_by_id,
        imdb_movies_lookup=imdb_movies_lookup,
        rt_lookup=rt_lookup
    )

    load_into_mongodb(documents)


if __name__ == "__main__":
    main()
