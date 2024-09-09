-- Project: Dashboard Requirement: Customer, Order, and Product Performance Analytics

-- Requested By: Sales/Marketing department
-- Due Date: [Provide a reasonable timeline]
-- Assigned to: Business Intelligence Team
-- Author: Solomon Banuba

-- DoD:
/*Key Metrics for the Dashboard:
Customer Insights:

Number of active customers by region (city, state, country).
Total orders placed by each customer.
Average revenue per customer.
Order Processing Efficiency:

Average time to ship orders (days_to_ship) for different regions or customer segments.
Order status breakdown (pending, shipped, completed).
Payment method distribution (e.g., Credit Card, PayPal, etc.).
Product Performance:

Revenue by product and product category.
Quantity sold per product.
Average discount applied per product.
Overall Revenue:

Total revenue over time (by day, week, month).
Revenue growth trends over time.
*/

-- ETL Pipeline to scheduled to run every 24 hours.

WITH customers AS (  -- Retrieving customer data
    SELECT 
       DISTINCT c.id AS customer_id
       ,  c.first_name AS first_name
       ,  c.last_name AS last_name
       ,  c.city AS city
       ,  c.state_province AS state
       ,  c.country_region AS country
    FROM `dl_northwind.customers` c
) 

, orders AS (-- Getting order information and doing data cleaning
    SELECT 
       o.customer_id AS customer_id
       ,  od.product_id AS product_id
       ,  DATETIME(TIMESTAMP(order_date), "Europe/Berlin") AS order_date -- Convert time to berlin time zone
       ,  DATETIME(TIMESTAMP(shipped_date), "Europe/Berlin") AS shipped_date -- Convert time to berlin time zone
       ,  COALESCE(o.payment_type, 'NA') AS payment_type -- Replacing Null with NA
       ,  ods.status AS status
       ,  od.quantity AS quantity
       ,  od.unit_price AS price 
       ,  od.discount AS discount
       ,  ROUND((od.quantity * od.unit_price) - (od.quantity * od.unit_price) * od.discount, 2) AS revenue
    FROM `dl_northwind.orders`o
    LEFT JOIN `dl_northwind.order_details_status` ods ON o.status_id = ods.id
    INNER JOIN `dl_northwind.order_details_` od ON o.id = od.order_id -- to filter customers who make at least one order
)

,  products AS (-- Removing 'Northwind Traders' from product name
    SELECT 
       p.id AS product_id
       , REPLACE(p.product_name, 'Northwind Traders ', '') AS product_name -- removing the string 'Northwind Traders'from product name
       , p.category
    FROM `dl_northwind.products` p 
    WHERE p.product_name LIKE 'Northwind Traders%' 
)

    SELECT 
      ct.customer_id
      , ct.first_name
      , ct.last_name
      , ot.order_date
      , ot.shipped_date
      , DATE_DIFF(ot.shipped_date, ot.order_date, DAY) AS days_to_ship
      , ct.city
      , ct.state
      , ct.country
      , ot.payment_type
      , ot.status
      , pr.product_name
      , pr.category
      , ot.quantity
      , ot.price
      , ot.discount
      , ot.revenue
    FROM customers ct
    INNER JOIN orders ot ON ct.customer_id = ot.customer_id
    LEFT JOIN products pr ON pr.product_id = ot.product_id; 
