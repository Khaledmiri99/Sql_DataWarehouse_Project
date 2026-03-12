/*
===============================================================================
Data Segmentation Analysis
===============================================================================
Purpose:
    - To group data into meaningful categories for targeted insights.
    - For customer segmentation, product categorization, or regional analysis.

SQL Functions Used:
    - CASE: Defines custom segmentation logic.
    - GROUP BY: Groups data into segments.
===============================================================================
*/

/*Segment products into cost ranges and 
count how many products fall into each segment*/

WITH product_segment AS (
SELECT 
product_key,
product_name,
cost,
CASE WHEN cost < 100 THEN 'Below 100'
	WHEN cost BETWEEN 100 AND 500 THEN '100-500'
	WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 1000'
	END cost_range
FROM gold.dim_products )

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC




/*Group customers into three segments based on their spending behavior 
	- VIP: Customers with at least 12 months of history and spending more than 5000 euros 
	- Regular: Customers at least 12 months of history but spending 5000 euros or less
	- New: Customers with lifespan less than 12 months 
And find the total number of customers by each group
*/ 

SELECT 
customer_rank,
COUNT(customer_key) AS total_customers
FROM ( 

WITH customer_spending AS (
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) AS first_order_date,
MAX(order_date) AS last_order_date,
(EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
(EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS lifespan
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key
ORDER BY c.customer_key
)

SELECT 
customer_key,
total_spending,
lifespan,
CASE WHEN lifespan >=12 AND total_spending > 5000 THEN 'VIP'
     WHEN lifespan >=12 AND total_spending <= 5000 THEN 'Regular'
	 ELSE 'New'
	 END AS customer_rank
FROM customer_spending )

GROUP BY customer_rank
ORDER BY total_customers DESC