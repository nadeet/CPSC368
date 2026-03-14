# Oracle Phase 3 Python Files

These files were created from the notebook `phase3_coba_newest.ipynb` and split into
the two Python deliverables requested by the rubric.

## Files

- `build_oracle_database.py`
  - Creates the Oracle tables
  - Reads the cleaned CSV files
  - Cleans/filter the data
  - Inserts the records into Oracle

- `run_oracle_analysis.py`
  - Connects to Oracle
  - Runs the SQL queries from the notebook
  - Retrieves the results into pandas DataFrames
  - Prints the final tables
  - Saves the final tables to:
    - `query_a_results.csv`
    - `query_b_results.csv`
    - `query_c_results.csv`

- `requirements.txt`
  - Lists the external Python packages used

## External libraries used

Beyond Oracle itself, the notebook uses:

- `oracledb`
- `pandas`

Install them with:

```bash
pip install -r requirements.txt
```

## How to run

1. Put these Python files in the same folder as:
   - `selected_letterboxd_movies.csv`
   - `selected_letterboxd_genres.csv`
   - `studios.csv`
   - `selected_movie_industry.csv`
   - `selected_rotten_tomato_movies.csv`

2. Update the Oracle credentials inside both Python files:
   - `user="ora_CWL"`
   - `password="aSTU_ID"`

3. Run the database build/load script:

```bash
python build_oracle_database.py
```

4. Run the analysis script:

```bash
python run_oracle_analysis.py
```

## Fidelity to the notebook

The SQL queries and overall workflow were kept the same as the notebook.

The only changes made were:
- splitting the notebook into two required submission files
- adding comments and a `main()` function
- fixing one notebook cell's line-break formatting so it runs as valid Python
- adding printed output / CSV export so the results appear when run outside Jupyter
