This project is a small end-to-end data engineering pipeline I built to practice working with real data workflows. It generates synthetic trip data, cleans and transforms it, and then loads it into a DuckDB database where you can query daily revenue and other metrics. Even though the dataset is small, I wanted to design it in a way that resembles how an actual pipeline works.

Tech Stack: Python, Pandas, DuckDB

🔁 Pipeline Steps

Ingest: create synthetic CSV files of trip data

Transform: clean the data, calculate features (duration, revenue), and store a processed dataset

Load: write the results to DuckDB and build a simple analytics view
