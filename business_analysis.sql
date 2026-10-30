-- ====================================================================
-- BUSINESS ANALYSIS SQL QUERIES
-- Database: food_orders_analytics_db
-- Target Table: cleaned_food_orders
-- Description: Answers core business performance and revenue questions.
-- ====================================================================

USE food_orders_analytics_db;

-- --------------------------------------------------------------------
-- Query 1: What is the total order value?
-- Excludes missing order values to preserve financial accuracy.
-- --------------------------------------------------------------------
SELECT 
    ROUND(SUM(order_value), 2) AS total_order_value,
    COUNT(order_value) AS orders_with_value_count,
    COUNT(*) - COUNT(order_value) AS missing_order_value_count
FROM cleaned_food_orders;

-- --------------------------------------------------------------------
-- Query 2: What is the average order value?
-- Computes the mean across valid (non-null) order values.
-- --------------------------------------------------------------------
SELECT 
    ROUND(AVG(order_value), 2) AS average_order_value
FROM cleaned_food_orders
WHERE order_value IS NOT NULL;

-- --------------------------------------------------------------------
-- Query 3: Which payment method has the highest number of orders?
-- Excludes 'Unknown' imputed values to reflect genuine payment methods.
-- --------------------------------------------------------------------
SELECT 
    payment_method, 
    COUNT(*) AS total_orders
FROM cleaned_food_orders
WHERE payment_method != 'Unknown'
GROUP BY payment_method
ORDER BY total_orders DESC
LIMIT 1;

-- --------------------------------------------------------------------
-- Query 3b (Supplementary): Complete payment method breakdown
-- --------------------------------------------------------------------
SELECT 
    payment_method, 
    COUNT(*) AS total_orders,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_food_orders), 2) AS percentage
FROM cleaned_food_orders
GROUP BY payment_method
ORDER BY total_orders DESC;

-- --------------------------------------------------------------------
-- Query 4: Which order channel has the highest number of orders?
-- Identifies the primary customer ordering platform.
-- --------------------------------------------------------------------
SELECT 
    order_channel, 
    COUNT(*) AS total_orders
FROM cleaned_food_orders
GROUP BY order_channel
ORDER BY total_orders DESC
LIMIT 1;

-- --------------------------------------------------------------------
-- Query 4b (Supplementary): Complete order channel breakdown
-- --------------------------------------------------------------------
SELECT 
    order_channel, 
    COUNT(*) AS total_orders,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM cleaned_food_orders), 2) AS percentage
FROM cleaned_food_orders
GROUP BY order_channel
ORDER BY total_orders DESC;

-- --------------------------------------------------------------------
-- Query 5: What is the average delivery time in minutes?
-- Calculated from (delivery_date_time - order_date_time) in minutes.
-- --------------------------------------------------------------------
SELECT 
    ROUND(AVG(delivery_time_minutes), 2) AS avg_delivery_time_minutes,
    ROUND(MIN(delivery_time_minutes), 2) AS min_delivery_time_minutes,
    ROUND(MAX(delivery_time_minutes), 2) AS max_delivery_time_minutes
FROM cleaned_food_orders;

-- --------------------------------------------------------------------
-- Query 6: What is the total platform net revenue according to the formula?
-- Formula: Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks
-- Includes only orders where all four financial components were present.
-- --------------------------------------------------------------------
SELECT 
    ROUND(SUM(platform_net_revenue), 2) AS total_platform_net_revenue,
    COUNT(platform_net_revenue) AS orders_included_in_revenue,
    (SELECT COUNT(*) FROM cleaned_food_orders) - COUNT(platform_net_revenue) AS orders_excluded_due_to_null_components
FROM cleaned_food_orders;
