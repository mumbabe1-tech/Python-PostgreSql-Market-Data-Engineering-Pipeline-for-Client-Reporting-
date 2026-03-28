# stock_pipeline_validation.py

import psycopg2

def validate_stock_data():
    """
    Performs several data quality checks on the stock_prices table.
    Ensures that the data loaded for the current date is complete,
    valid, and free from common data quality issues.
    """

    conn = psycopg2.connect(
        host="airflow-db",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    
    #  Check today's data has been loaded into the table.

    cur.execute("""
        SELECT COUNT(*) 
        FROM stockPrices_data 
        WHERE date = CURRENT_DATE;
    """)
    row_count = cur.fetchone()[0]

    if row_count == 0:
        raise ValueError("Validation failed: No stock data found for today's date.")
    else:
        print(f"Validation passed: {row_count} rows found for today's date.")

   
  #  Check for NULL or missing price values.
  
    cur.execute("""
        SELECT COUNT(*) 
        FROM stockPrices_data 
        WHERE price IS NULL;
    """)
    null_count = cur.fetchone()[0]

    if null_count > 0:
        raise ValueError(f"Validation failed: {null_count} rows contain NULL prices.")
    else:
        print("Validation passed: No NULL price values detected.")

   
    # Check for negative price values.

    cur.execute("""
        SELECT COUNT(*) 
        FROM stockPrices_data 
        WHERE price < 0;
    """)
    negative_count = cur.fetchone()[0]

    if negative_count > 0:
        raise ValueError(f"Validation failed: {negative_count} rows contain negative prices.")
    else:
        print("Validation passed: No negative price values detected.")

 
    # Check for duplicate timestamps.
    
    cur.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT timestamp, COUNT(*)
            FROM stockPrices_data
            GROUP BY timestamp
            HAVING COUNT(*) > 1
        ) duplicates;
    """)
    duplicate_count = cur.fetchone()[0]

    if duplicate_count > 0:
        raise ValueError(f"Validation failed: {duplicate_count} duplicate timestamp entries detected.")
    else:
        print("Validation passed: No duplicate timestamps found.")

    conn.close()

  