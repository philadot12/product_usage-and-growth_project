import mysql.connector
import pandas as pd
from datetime import datetime
import os
import time
from typing import Dict, Any, List

# --- Configuration ---
# WARNING: REPLACE THE PLACEHOLDER VALUES BELOW with your actual MySQL credentials.
# The script will fail if these are not correct.
DB_CONFIG: Dict[str, str] = {
    'host': 'localhost',
    'user': 'root',        # <--- REPLACE WITH YOUR MYSQL USERNAME
    'password': 'Emmanuelfc@12',  # <--- REPLACE WITH YOUR MYSQL PASSWORD
    'database': 'projects' # <--- REPLACE WITH YOUR DATABASE NAME
}

# Directory to save the output CSV files
OUTPUT_DIR: str = 'Product_Usage_Report' 
MAX_RETRIES: int = 5
RETRY_DELAY_SECONDS: int = 2

# Ensure the output directory exists
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Created output directory: {OUTPUT_DIR}")

# --- SQL Queries for Automated Analysis ---
# These queries aggregate data for the Power BI Dashboard.
QUERIES: Dict[str, str] = {
    # Query 2: Monthly Financial KPIs
    'monthly_financial_kpis': """
        SELECT
             order_month,
            COUNT(DISTINCT order_id) AS TotalTransactions,
            SUM(CASE
                WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
                ELSE payment_amount_ghs
            END) AS TotalRevenueGHS,
            ROUND(SUM(CASE
                WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
                ELSE payment_amount_ghs
            END) / COUNT(DISTINCT order_id), 2) AS AOV_GHS
        FROM
            ProductUsage
        GROUP BY
            order_month
        ORDER BY
            TotalRevenueGHS;
    """,

    # Query 3a: Top 10 Products by Revenue
    'top_10_products_revenue': """
        SELECT
            product_id,
            product_name,
            category,
            brand,
            COUNT(DISTINCT order_id) AS OrdersCount,
            SUM(quantity) AS TotalUnitsSold,
            SUM(total_item_value_ghs) AS ProductRevenue
        FROM
            ProductUsage
        GROUP BY
            product_id, product_name, category, brand
        ORDER BY
            ProductRevenue DESC
        LIMIT 10;
    """,

    # Query 3b: Revenue by Category
    'category_revenue': """
        SELECT
            category,
            SUM(CASE
                WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
                ELSE payment_amount_ghs
            END) AS CategoryRevenue
        FROM
            ProductUsage
        GROUP BY
            category
        ORDER BY
            CategoryRevenue DESC;
    """,

    # Query 4a: Regional Revenue
    'city_revenue': """
        SELECT
            city,
            COUNT(DISTINCT order_id) AS TotalOrders,
            SUM(CASE
                WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
                ELSE payment_amount_ghs
            END) AS cityRevenue
        FROM
            ProductUsage
        GROUP BY
            city
        ORDER BY
            cityRevenue DESC;
    """,

    # Query 4b: Payment Method Performance
    'payment_method_performance': """
        SELECT
            payment_method,
            COUNT(DISTINCT order_id) AS OrdersCount,
            SUM(CASE
                WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
                ELSE payment_amount_ghs
            END) AS TotalRevenue
        FROM
            ProductUsage
        WHERE
            payment_method IS NOT NULL AND payment_method != ''
        GROUP BY
            payment_method
        ORDER BY
            TotalRevenue DESC;
    """,

    # Query 5a: Monthly Active Users (MAU)
    'monthly_active_users': """
        SELECT
            DATE_FORMAT(STR_TO_DATE(order_date, '%m/%d/%Y %H:%i'), '%Y-%m') AS activity_month,
            COUNT(DISTINCT user_id) AS MAU
        FROM
            ProductUsage
        GROUP BY
            activity_month
        ORDER BY
            activity_month;
    """,

    # Query 5b: New User Acquisition (Count by Month Name)
    'new_user_acquisition_words': """
        WITH UserFirstOrder AS (
            SELECT
                user_id,
                MIN(STR_TO_DATE(order_date, '%m/%d/%Y %H:%i')) AS FirstOrderDate
            FROM
                ProductUsage
            GROUP BY
                user_id
        )
        SELECT
            -- Display month in words (e.g., 'August 2025')
            DATE_FORMAT(FirstOrderDate, '%M %Y') AS AcquisitionMonth,
            COUNT(user_id) AS NewUsers,
            -- Include sortable date for BI tools to correctly order the months chronologically
            DATE_FORMAT(FirstOrderDate, '%Y-%m') AS AcquisitionSortKey
        FROM
            UserFirstOrder
        GROUP BY
            AcquisitionMonth,
            AcquisitionSortKey
        ORDER BY
            AcquisitionSortKey;
    """,

    # Query 6a: User Conversion Segments
    'user_conversion_segments': """
        SELECT
            CASE
                WHEN views_count > 0 AND COUNT(DISTINCT order_id) > 0 THEN 'Viewer & Purchaser'
                WHEN views_count > 0 AND COUNT(DISTINCT order_id) = 0 THEN 'Viewer Only'
                ELSE 'Purchaser Only'
            END AS UserSegment,
            COUNT(DISTINCT user_id) AS UserCount,
            SUM(CASE
                WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
                ELSE payment_amount_ghs
            END) AS TotalRevenue
        FROM
            ProductUsage
        GROUP BY
            UserSegment;
    """,

    # Query 6b: Average Views per Purchased Product
    'avg_views_before_purchase': """
        SELECT
            AVG(views_count) AS AverageViewsBeforePurchase,
            COUNT(DISTINCT product_id) AS TotalPurchasedProducts
        FROM
            ProductUsage
        WHERE
            views_count > 0;
    """
}

def execute_query_with_retry(cursor: Any, query_sql: str) -> List[Dict[str, Any]]:
    """
    Executes a single SQL query with exponential backoff for connection stability.
    """
    for attempt in range(MAX_RETRIES):
        try:
            cursor.execute(query_sql)
            return cursor.fetchall()
        except mysql.connector.Error as err:
            if attempt < MAX_RETRIES - 1:
                print(f"    SQL Error (Attempt {attempt + 1}/{MAX_RETRIES}): {err}. Retrying in {RETRY_DELAY_SECONDS ** (attempt + 1)}s...")
                time.sleep(RETRY_DELAY_SECONDS ** (attempt + 1))
            else:
                print(f"    FATAL SQL Error after {MAX_RETRIES} attempts: {err}")
                return []
        except Exception as e:
            print(f"    An unexpected error occurred during execution: {e}")
            return []
    return []


def run_analysis_and_generate_reports():
    """
    Connects to MySQL, runs defined queries, and saves results to CSV files.
    """
    print(f"[{datetime.now()}] Starting automated analysis pipeline...")
    connection = None
    cursor = None

    try:
        # Establish connection to MySQL
        print("  Attempting to connect to MySQL database...")
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor(dictionary=True) # returns rows as dictionaries

        if connection.is_connected():
            print("  Successfully connected to database.")
        else:
            print("  Failed to establish database connection.")
            return

        for query_name, query_sql in QUERIES.items():
            print(f"  Executing query: {query_name}...")

            # Use the robust execution function
            results = execute_query_with_retry(cursor, query_sql)

            if results:
                df = pd.DataFrame(results)
                output_filename = os.path.join(OUTPUT_DIR, f'{query_name}.csv')
                
                # Save the DataFrame to CSV, overwriting the old file
                df.to_csv(output_filename, index=False)
                print(f"  Saved {len(df)} rows to {output_filename}")
            else:
                print(f"  No data returned or query failed for {query_name}. Skipping file generation.")

    except mysql.connector.Error as err:
        print(f"\n[CRITICAL FAILURE] Database connection error: {err}")
        print("Please check your DB_CONFIG (host, user, password, database name) in the script.")
    except Exception as e:
        print(f"\n[CRITICAL FAILURE] An unexpected error occurred: {e}")
    finally:
        # Close connection gracefully
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()
            print("  MySQL connection closed.")
        
        print(f"\n[{datetime.now()}] Automated analysis pipeline finished.")


if __name__ == '__main__':
    # Add a visual element to represent the data flow from SQL to CSV
    print("\n----------------------------------------------------------------------")
    print("  PYTHON AUTOMATION SCRIPT")
    print("  Data Flow: MySQL Database -> Python/Pandas -> Aggregated CSV Files")
    print("----------------------------------------------------------------------")
    # 
    run_analysis_and_generate_reports()