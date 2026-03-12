-- Analyse sales performance over time

SELECT 
	EXTRACT(YEAR FROM order_date ) AS order_date_year, --Year granularity
	EXTRACT(MONTH FROM order_date ) AS order_date_month, --Month granularity
	SUM(sales_amount) AS total_sales_amount_per_year,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM (amount) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY order_date_year, order_date_month
ORDER BY order_date_year, order_date_month
