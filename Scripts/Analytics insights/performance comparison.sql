/*
===============================================================================
Performance Analysis (Year-over-Year, Month-over-Month)
===============================================================================

Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */

WITH yearly_product_sales AS ( --CTE
SELECT 
TO_CHAR(f.order_date, 'YYYY') as order_date,
p.product_name,
SUM(f.sales_amount) as current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date IS NOT NULL
GROUP BY p.product_name, TO_CHAR(f.order_date, 'YYYY'))

SELECT 
order_date,
product_name,
current_sales,
AVG(current_sales) OVER (PARTITION BY product_name) as average_sales,
current_sales - AVG(current_sales) OVER (PARTITION BY product_name) as diff_avg,
CASE WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) > 0 THEN 'Above Average'
	WHEN current_sales - AVG(current_sales) OVER (PARTITION BY product_name) < 0 THEN 'Below Average'
	ELSE 'On Average'
	END AS average_change,
--year-over-year Analysis
LAG (current_sales) OVER (PARTITION BY product_name ORDER BY order_date) as previous_year_sales,
current_sales - LAG (current_sales) OVER (PARTITION BY product_name ORDER BY order_date) as diff_py_sales,
CASE WHEN current_sales - LAG (current_sales) OVER (PARTITION BY product_name ORDER BY order_date) > 0 THEN 'Increasing'
	WHEN current_sales - LAG (current_sales) OVER (PARTITION BY product_name ORDER BY order_date) < 0 THEN 'Descreasing'
	ELSE 'No change'
	END AS sales_variation
FROM yearly_product_sales
ORDER BY product_name, order_date