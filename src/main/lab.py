import pandas as pd
import sqlite3

def process_data():
    # -------------------------------------------------------------------------
    # Step 1: Data Loading and Cleaning
    # -------------------------------------------------------------------------
    # Load the transactions data from the CSV file.
    file_path = r"src/data/transactions.csv"
    df = pd.read_csv(file_path, encoding="utf-8")
    
    # Remove rows with missing values to clean the dataset.
    df.dropna(inplace=True)
    
    # Convert the 'TransactionDate' column to a datetime datatype.
    df["TransactionDate"] = pd.to_datetime(df["TransactionDate"])
    
    # -------------------------------------------------------------------------
    # Step 2: Create SQLite Database and Table
    # -------------------------------------------------------------------------
    # Establish a connection to the SQLite database.
    conn = sqlite3.connect("src/data/transactions.db")
    cursor = conn.cursor()
    
    # Create the 'transactions' table with the required structure.
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        TransactionID INTEGER PRIMARY KEY,
        CustomerID INTEGER,
        Product TEXT,
        Amount REAL,
        TransactionDate TEXT,
        PaymentMethod TEXT,
        City TEXT,
        Category TEXT
    )
    """)
    
    # -------------------------------------------------------------------------
    # Step 3: Insert Data into SQLite Database
    # -------------------------------------------------------------------------
    # Insert the cleaned DataFrame into the transactions table.
    # Using if_exists="replace" ensures that old data is replaced.
    df.to_sql("transactions", conn, if_exists="replace", index=False)
    
    # -------------------------------------------------------------------------
    # Step 4: Perform SQL Queries for Data Analysis
    # -------------------------------------------------------------------------
    
    # 1. Top 5 Best-Selling Products: Identify products by sales count.
    query_top_products = """
    SELECT Product, COUNT(*) AS SalesCount
    FROM transactions
    GROUP BY Product
    ORDER BY SalesCount DESC
    LIMIT 5;
    """
    cursor.execute(query_top_products)
    top_products = cursor.fetchall()
    print("Top 5 Best-Selling Products:")
    for product, sales_count in top_products:
        print(f"Product: {product}, Sales Count: {sales_count}")
    
    # 2. Monthly Revenue Trend: Calculate total revenue per month.
    query_monthly_revenue = """
    SELECT strftime('%Y-%m', TransactionDate) AS Month, SUM(Amount) AS TotalRevenue
    FROM transactions
    GROUP BY Month
    ORDER BY Month;
    """
    cursor.execute(query_monthly_revenue)
    monthly_revenue = cursor.fetchall()
    print("\nMonthly Revenue Trend:")
    for month, revenue in monthly_revenue:
        print(f"Month: {month}, Total Revenue: {revenue}")
    
    # 3. Payment Method Popularity: Count transactions per payment method.
    query_payment_methods = """
    SELECT PaymentMethod, COUNT(*) AS TransactionCount
    FROM transactions
    GROUP BY PaymentMethod
    ORDER BY TransactionCount DESC;
    """
    cursor.execute(query_payment_methods)
    payment_methods = cursor.fetchall()
    print("\nPayment Method Popularity:")
    for method, count in payment_methods:
        print(f"Payment Method: {method}, Transaction Count: {count}")
    
    # 4. Top Cities with Most Transactions: List cities with the highest numbers.
    query_top_cities = """
    SELECT City, COUNT(*) AS TransactionCount
    FROM transactions
    GROUP BY City
    ORDER BY TransactionCount DESC
    LIMIT 5;
    """
    cursor.execute(query_top_cities)
    top_cities = cursor.fetchall()
    print("\nTop Cities with Most Transactions:")
    for city, count in top_cities:
        print(f"City: {city}, Transactions: {count}")
    
    # 5. Top Spending Customers: Identify customers who spent the most.
    query_top_customers = """
    SELECT CustomerID, SUM(Amount) AS TotalSpent
    FROM transactions
    GROUP BY CustomerID
    ORDER BY TotalSpent DESC
    LIMIT 5;
    """
    cursor.execute(query_top_customers)
    top_customers = cursor.fetchall()
    print("\nTop Spending Customers:")
    for customer, total in top_customers:
        print(f"Customer ID: {customer}, Total Spent: {total}")
    
    # 6. Hadoop vs Spark Related Product Sales: Compare sales for Hadoop/Spark keywords.
    query_hadoop_vs_spark = """
    SELECT CASE
             WHEN LOWER(Product) LIKE '%hadoop%' THEN 'Hadoop'
             WHEN LOWER(Product) LIKE '%spark%' THEN 'Spark'
             ELSE 'Other'
           END AS ProductGroup,
           COUNT(*) AS SalesCount
    FROM transactions
    GROUP BY ProductGroup;
    """
    cursor.execute(query_hadoop_vs_spark)
    hadoop_spark = cursor.fetchall()
    print("\nSales Comparison for Hadoop vs Spark Related Products:")
    for group, sales in hadoop_spark:
        print(f"Group: {group}, Sales Count: {sales}")
    
    # 7. Top Spending Customer in Each City: Use subqueries to determine the highest spender per city.
    query_top_customer_each_city = """
    SELECT t.City, t.CustomerID, t.TotalSpent
    FROM (
        SELECT City, CustomerID, SUM(Amount) AS TotalSpent
        FROM transactions
        GROUP BY City, CustomerID
    ) t
    JOIN (
        SELECT City, MAX(TotalSpent) AS MaxSpent
        FROM (
            SELECT City, CustomerID, SUM(Amount) AS TotalSpent
            FROM transactions
            GROUP BY City, CustomerID
        )
        GROUP BY City
    ) m ON t.City = m.City AND t.TotalSpent = m.MaxSpent;
    """
    cursor.execute(query_top_customer_each_city)
    top_each_city = cursor.fetchall()
    print("\nTop Spending Customer in Each City:")
    for city, customer, spent in top_each_city:
        print(f"City: {city}, Customer ID: {customer}, Total Spent: {spent}")
    
    # -------------------------------------------------------------------------
    # Final Steps: Commit and Close
    # -------------------------------------------------------------------------
    conn.commit()
    conn.close()
    print("\n✅ Data Processing & SQL Analysis Completed Successfully!")

if __name__ == "__main__":
    process_data()
