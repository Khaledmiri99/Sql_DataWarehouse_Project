DROP VIEW IF EXISTS gold.report_products;

CREATE VIEW gold.report_products AS
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_products
---------------------------------------------------------------------------*/
WITH base_query AS (
SELECT 
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.amount as quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
FROM gold.dim_products p
LEFT JOIN gold.fact_sales f
ON p.product_key = f.product_key
WHERE order_date IS NOT NULL
),

/*---------------------------------------------------------------------------
2) Product Aggregations: Summarizes key metrics at the product level
---------------------------------------------------------------------------*/
product_aggregations AS (
SELECT
	
	product_key,
	product_name,
	category,
	subcategory,
	COUNT(DISTINCT customer_key) AS total_customers,
	cost,
	(EXTRACT(YEAR FROM MAX(order_date)) - EXTRACT(YEAR FROM MIN(order_date))) * 12 +
	(EXTRACT(MONTH FROM MAX(order_date)) - EXTRACT(MONTH FROM MIN(order_date))) AS lifespan_in_months,
	MAX(order_date) AS last_order_date,
	COUNT(DISTINCT order_number) AS total_orders,
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	ROUND((SUM(sales_amount) / NULLIF(SUM(quantity), 0)), 1) as avg_selling_price
FROM base_query
GROUP BY 
		product_key,
		product_name,
		category,
		subcategory,
		cost
		
)

/*---------------------------------------------------------------------------
  3) Final Query: Combines all product results into one output
---------------------------------------------------------------------------*/

SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_order_date,
	EXTRACT(YEAR FROM AGE(last_order_date)) * 12 +
	EXTRACT(MONTH FROM AGE(last_order_date)) AS recency_in_months,
	CASE WHEN total_sales > 50000 THEN 'High Performer'
		 WHEN total_sales BETWEEN 10000 AND 50000 THEN 'Mid Range'
		 ELSE 'Low Performer'
	END AS product_segment,
	lifespan_in_months,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average order revenue (AOR-KPI)
	CASE WHEN total_orders = 0 THEN 0
	ELSE total_sales / total_orders
	END AS avg_order_revenue,
	-- Average monthly revenue (AMR-KPI)
	CASE WHEN lifespan_in_months = 0 THEN total_sales 
	ELSE total_sales / lifespan_in_months
	END AS avg_monthly_revenue
FROM product_aggregations

