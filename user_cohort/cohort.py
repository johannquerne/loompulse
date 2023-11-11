import mysql.connector
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='host',
    user='user',
    password='password',
    database='database'
)

cursor = conn.cursor()

# SQL query for user cohort and monthly sales for the last 12 months
cohort_query = """
WITH monthly_sales AS (ls
    SELECT
        UUID AS user_id,
        MIN(DATE_FORMAT(OrderDate, '%Y-%m-01')) AS cohort_month
    FROM
        orders
    WHERE
        OrderDate >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
    GROUP BY
        UUID
),
monthly_sales_data AS (
    SELECT
        UUID AS user_id,
        DATE_FORMAT(OrderDate, '%Y-%m-01') AS order_month,
        COUNT(*) AS monthly_sales
    FROM
        orders
    WHERE
        OrderDate >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
    GROUP BY
        UUID, order_month
)
SELECT
    c.cohort_month,
    msd.order_month,
    COUNT(DISTINCT c.user_id) AS cohort_size,
    COALESCE(SUM(msd.monthly_sales), 0) AS monthly_sales
FROM
    monthly_sales c
LEFT JOIN
    monthly_sales_data msd
ON
    c.user_id = msd.user_id
    AND c.cohort_month <= msd.order_month
GROUP BY
    c.cohort_month, msd.order_month
ORDER BY
    c.cohort_month, msd.order_month;
"""

# Create a DataFrame from the query result
cohort_data = pd.read_sql_query(cohort_query, conn)

# Format Amount in Euros
cohort_data['monthly_sales'] = cohort_data['monthly_sales'].astype(float)

# Pivot the cohort data into a cohort table
cohort_table = cohort_data.pivot(index='cohort_month', columns='order_month', values='monthly_sales')

# Create a Seaborn heatmap
plt.figure(figsize=(12, 8))
sns.heatmap(cohort_table, annot=True, cmap='YlGnBu', fmt='.0f', cbar=False)
plt.title('User Cohort Monthly Sales Analysis', fontsize=16)
plt.xlabel('Order Month')
plt.ylabel('Cohort Month')
plt.show()

# Close the database connection
conn.close()