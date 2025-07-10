This project solves a Spark-based data analysis task using real-world-style data from a loan brokerage scenario. It includes:

Analysis of applications, loans, and marketing sources
Integrity and quality checks (e.g., duplicates, orphans, outliers)
Visual insights using Matplotlib

Setup Instructions to run locally

1. Install Python & JDK
2. Install Dependencies & change the parquet path in the code to your path
pip install pyspark pandas matplotlib
3. The solution it's in the solution.py file

What it does:
Counts total applications
Calculates average profit from approved applications
Finds most/second-most popular marketing sources per loan
Calculates daily profit and daily target achievement per source
Covers basic data integrity checks for unique keys, outliers and joins
Visualize basic information 

Note on Data Integrity (Exercise 2):
While analyzing approved applications, I noticed that 1 loan is present in the loans table but has no matching approved application in the applications dataset.

