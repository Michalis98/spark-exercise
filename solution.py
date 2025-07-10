from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import sys
import matplotlib.pyplot as plt
import pandas as pd

spark = SparkSession.builder \
    .appName("SparkExercise") \
    .getOrCreate()

applications_parquet_path = r"C:\Users\michalis.a\Desktop\spark\spark-interview-2-\spark-interview\applications_parquet"
loans_parquet_path = r"C:\Users\michalis.a\Desktop\spark\spark-interview-2-\spark-interview\loans_parquet"
sources_parquet_path = r"C:\Users\michalis.a\Desktop\spark\spark-interview-2-\spark-interview\sources_parquet"


print(f"Attempting to read all Parquet files")

try:
    applications_data = spark.read.parquet(applications_parquet_path)
    loans_data = spark.read.parquet(loans_parquet_path)
    #casting commission column to double because I will do some calculations later
    loans_data = loans_data.withColumn("commission", col("commission").cast("double"))
    sources_data = spark.read.parquet(sources_parquet_path)
except Exception as e:
    print(f"\nAn error occurred while reading Parquet files: {e}")
    sys.exit(1)  # Stop execution if parquet loading fails
finally:  
    applications_data.createOrReplaceTempView("all_applications_data")
    loans_data.createOrReplaceTempView("all_loans_data")
    sources_data.createOrReplaceTempView("all_sources_data")

    #Data checks

    print(f"Duplicate Applications")
    spark.sql("""
    SELECT app_id as duplicate_application_id, COUNT(*) as count
    FROM all_applications_data
    GROUP BY app_id
    HAVING count > 1
    """).show()

    print(f"Referential integrity")
    spark.sql("""
    SELECT COUNT(*) as orphan_loans FROM all_applications_data a
    LEFT JOIN all_loans_data l ON a.loan_id = l.loan_id
    WHERE l.loan_id IS NULL
    """).show()

    print(f"Loan Status")
    spark.sql("""
    SELECT status, COUNT(*) as amount
    FROM all_applications_data
    GROUP BY status
    """).show()

    total_approved = applications_data.filter(col("status") == "approved").count()
    approval_rate = total_approved / applications_data.count()
    print(f"Approval Rate: {approval_rate * 100:.2f}%")


    print(f"Check for outliers")
    spark.sql("""
    SELECT 
    MIN(commission),
    MAX(commission),
    AVG(commission),
    PERCENTILE(commission, 0.95) -- if available
    FROM all_loans_data
    """).show()

   


    # Exercise 1 - How many applications have been submitted from the beginning of the time.

    print(f"Exercise 1")

    #app_id is distinct but doing it for safety
    spark.sql("SELECT count(distinct(app_id)) as Sumbitted_loans FROM all_applications_data").show()



    # Exercise 2 - How many applications have been submitted from the beginning of the time?
    # 2 approaches.
    # The first which I think is more correct is to join the applications with loans and get only the approved ones
    # The second is only to get the loans table because in theory in there we only have the approved loans

    joined_applications_loans = applications_data.join(loans_data,on='loan_id',how='inner')
    joined_applications_loans.cache() # caching for effieciency 
    joined_applications_loans.createOrReplaceTempView("applications_with_loans")

    # result = spark.sql("SELECT count(distinct(loan_id)) as total_loans FROM all_loans_data").show()
    # result = spark.sql("SELECT count(distinct(loan_id)) as total_approved_loans FROM applications_with_loans where status = 'approved'").show()

    print(f"This loan exists in loan table but in applications is not approved")
    spark.sql("""
    WITH approved_loans AS (
    SELECT DISTINCT loan_id 
    FROM applications_with_loans 
    WHERE status = 'approved'
    )

    -- Show loan_ids that are in all_loans_data but not in approved apps
    SELECT loan_id, lender, loan_name, commission
    FROM all_loans_data
    WHERE loan_id NOT IN (SELECT loan_id FROM approved_loans)
    """).show()

    #As we can see from here the second approach is not correct because we have 1 more loan in loans table
    #In my opinion is wrong and this loan should be removed, and only approved loans should be kept in that table since not all declined loans are presented there just 1
    #hence I think it's an outlier.

    print(f"Exercise 2")

    spark.sql("""
    SELECT 
        CONCAT(ROUND(AVG(commission), 2), ' €') AS Average_commission
    FROM applications_with_loans
    WHERE status = 'approved'
    """).show()


    #Exercise 3 which marketing sources are the first and second most popular for each loan type ?
    print(f"Exercise 3")
    
    # Join the above result with sources on 'source_idd'
    full_joined_data = joined_applications_loans.alias("app_loans") \
    .join(sources_data.alias("src"),
          col("app_loans.source_id") == col("src.source_id"),
          how="left") \
    .select(
        col("app_loans.loan_name"),
        col("app_loans.source_id").alias("app_source_id"),  # rename to avoid ambiguity
        col("src.source_name")
    )

    full_joined_data.cache()
    full_joined_data.createOrReplaceTempView("applications_loans_sources")

    #Here I used row number in order to be able to rank the most popular loans grouping them by loan_name and source.
    #After the CTE I used rank=1,rank=2 to get the 2 most populars. If we need more we can expand the logic.
    query = """
    WITH source_counts AS (
        SELECT
            loan_name,
            source_name,
            COUNT(*) AS application_count,
            ROW_NUMBER() OVER (PARTITION BY loan_name ORDER BY COUNT(*) DESC) as rank
        FROM applications_loans_sources
        GROUP BY loan_name, source_name
    )

    SELECT
        loan_name as Loan,
        MAX(CASE WHEN rank = 1 THEN source_name END) AS Most_Popular,
        MAX(CASE WHEN rank = 2 THEN source_name END) AS Second_Most_Popular
    FROM source_counts
    WHERE rank <= 2
    GROUP BY loan_name
    ORDER BY loan_name
    """

    spark.sql(query).show()

    #Exercise 4 - Provide a list that shows for each day what is the percentage of profit generated by each marketing source and to what percentage did they 
    #reach their daily target

    print(f"Exercise 4")

    #Again in this query I will use a CTE to produce a table with the sum comission of each source each date
    #Later I will join source table to devide the profit with the daily target in order to get the daily Target

    query = """
    WITH source_counts AS (
        SELECT
            source_id,
            date,
            sum(commission) as profit
        FROM applications_with_loans
        where status='approved'
        GROUP BY  source_id, date
        ORDER BY 2,3
    )

    SELECT 
    date as Date,
    source_name as Source,
    profit as Profit,
    CONCAT(Profit/daily_target*100,'%') as Daily_Target
        From source_counts sc
    LEFT JOIN all_sources_data asd on sc.source_id = asd.source_id
    ORDER BY 1,2
    """

    spark.sql(query).show()


#######################################################################################################################################
###################################################EXTRA###############################################################################


    daily_profit = spark.sql("""
    SELECT 
        date, 
        sum(CASE WHEN status='approved' THEN commission ELSE 0 END) as daily_profit
    FROM applications_with_loans
    GROUP BY date
    ORDER BY date
    """).toPandas()

    # Convert to datetime
    daily_profit["date"] = pd.to_datetime(daily_profit["date"])

    plt.figure(figsize=(10, 5))
    plt.plot(daily_profit["date"], daily_profit["daily_profit"], marker='o')
    plt.title("Daily Profit Over Time")
    plt.xlabel("Date")
    plt.ylabel("Profit (£)")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig("daily_profit_line.png")
    plt.show()


    #############################################################################################

    status_counts = applications_data.groupBy("status").count().toPandas()

    plt.figure(figsize=(6, 6))
    plt.pie(status_counts["count"], labels=status_counts["status"], autopct='%1.1f%%', startangle=140)
    plt.title("Loan Application Status Distribution")
    plt.axis('equal')
    plt.tight_layout()
    plt.savefig("status_pie_chart.png")
    plt.show()


