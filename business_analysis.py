"""
Business Analysis Module for Food Orders Pipeline
==================================================
Executes SQL queries against the 'food_orders_analytics_db' database
to answer the six core business questions.
Outputs formatted analysis to stdout and writes to sql_analysis_results.txt.
"""

from typing import Dict, Any
from pathlib import Path
import mysql.connector

from config import (
    get_analytics_db_config,
    ANALYTICS_TABLE_NAME,
    SQL_ANALYSIS_PATH
)


def run_business_analysis() -> str:
    """
    Connects to the analytics database, runs the 6 required business queries,
    and returns a formatted string containing the answers.
    """
    conn = mysql.connector.connect(**get_analytics_db_config())
    cursor = conn.cursor(dictionary=True)

    output_lines = []
    output_lines.append("=" * 70)
    output_lines.append("FOOD ORDERS ANALYTICS - BUSINESS QUESTIONS SQL ANALYSIS")
    output_lines.append("=" * 70)

    # -------------------------------------------------------------
    # Query 1: Total Order Value
    # -------------------------------------------------------------
    q1 = f"""
        SELECT 
            ROUND(SUM(order_value), 2) AS total_order_value,
            COUNT(order_value) AS non_null_count,
            COUNT(*) - COUNT(order_value) AS null_count
        FROM {ANALYTICS_TABLE_NAME};
    """
    cursor.execute(q1)
    res1 = cursor.fetchone()
    total_val = res1["total_order_value"]
    output_lines.append(f"\n1. What is the total order value?")
    output_lines.append(f"   -> Total Order Value: ${total_val:,.2f}")
    output_lines.append(f"      (Based on {res1['non_null_count']} valid orders; {res1['null_count']} records with missing order values excluded)")

    # -------------------------------------------------------------
    # Query 2: Average Order Value
    # -------------------------------------------------------------
    q2 = f"""
        SELECT 
            ROUND(AVG(order_value), 2) AS average_order_value
        FROM {ANALYTICS_TABLE_NAME}
        WHERE order_value IS NOT NULL;
    """
    cursor.execute(q2)
    res2 = cursor.fetchone()
    avg_val = res2["average_order_value"]
    output_lines.append(f"\n2. What is the average order value?")
    output_lines.append(f"   -> Average Order Value: ${avg_val:,.2f}")

    # -------------------------------------------------------------
    # Query 3: Top Payment Method
    # -------------------------------------------------------------
    q3_top = f"""
        SELECT 
            payment_method, 
            COUNT(*) AS total_orders
        FROM {ANALYTICS_TABLE_NAME}
        WHERE payment_method != 'Unknown'
        GROUP BY payment_method
        ORDER BY total_orders DESC
        LIMIT 1;
    """
    cursor.execute(q3_top)
    res3 = cursor.fetchone()

    q3_all = f"""
        SELECT 
            payment_method, 
            COUNT(*) AS total_orders,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {ANALYTICS_TABLE_NAME}), 1) AS pct
        FROM {ANALYTICS_TABLE_NAME}
        GROUP BY payment_method
        ORDER BY total_orders DESC;
    """
    cursor.execute(q3_all)
    all_pm = cursor.fetchall()

    output_lines.append(f"\n3. Which payment method has the highest number of orders?")
    output_lines.append(f"   -> Top Payment Method: {res3['payment_method']} ({res3['total_orders']} orders)")
    output_lines.append("      Payment Methods Breakdown:")
    for pm in all_pm:
        output_lines.append(f"        - {pm['payment_method']:<18}: {pm['total_orders']:>3} orders ({pm['pct']}%)")

    # -------------------------------------------------------------
    # Query 4: Top Order Channel
    # -------------------------------------------------------------
    q4_top = f"""
        SELECT 
            order_channel, 
            COUNT(*) AS total_orders
        FROM {ANALYTICS_TABLE_NAME}
        GROUP BY order_channel
        ORDER BY total_orders DESC
        LIMIT 1;
    """
    cursor.execute(q4_top)
    res4 = cursor.fetchone()

    q4_all = f"""
        SELECT 
            order_channel, 
            COUNT(*) AS total_orders,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {ANALYTICS_TABLE_NAME}), 1) AS pct
        FROM {ANALYTICS_TABLE_NAME}
        GROUP BY order_channel
        ORDER BY total_orders DESC;
    """
    cursor.execute(q4_all)
    all_oc = cursor.fetchall()

    output_lines.append(f"\n4. Which order channel has the highest number of orders?")
    output_lines.append(f"   -> Top Order Channel: {res4['order_channel']} ({res4['total_orders']} orders)")
    output_lines.append("      Order Channels Breakdown:")
    for oc in all_oc:
        output_lines.append(f"        - {oc['order_channel']:<18}: {oc['total_orders']:>3} orders ({oc['pct']}%)")

    # -------------------------------------------------------------
    # Query 5: Average Delivery Time
    # -------------------------------------------------------------
    q5 = f"""
        SELECT 
            ROUND(AVG(delivery_time_minutes), 2) AS avg_delivery_time,
            ROUND(MIN(delivery_time_minutes), 2) AS min_delivery_time,
            ROUND(MAX(delivery_time_minutes), 2) AS max_delivery_time
        FROM {ANALYTICS_TABLE_NAME};
    """
    cursor.execute(q5)
    res5 = cursor.fetchone()
    output_lines.append(f"\n5. What is the average delivery time in minutes?")
    output_lines.append(f"   -> Average Delivery Time: {res5['avg_delivery_time']} minutes "
                        f"(Range: {res5['min_delivery_time']} - {res5['max_delivery_time']} mins)")

    # -------------------------------------------------------------
    # Query 6: Total Platform Net Revenue
    # -------------------------------------------------------------
    q6 = f"""
        SELECT 
            ROUND(SUM(platform_net_revenue), 2) AS total_platform_net_revenue,
            COUNT(platform_net_revenue) AS included_orders,
            (SELECT COUNT(*) FROM {ANALYTICS_TABLE_NAME}) - COUNT(platform_net_revenue) AS excluded_orders
        FROM {ANALYTICS_TABLE_NAME};
    """
    cursor.execute(q6)
    res6 = cursor.fetchone()
    output_lines.append(f"\n6. What is the total platform net revenue according to the specified formula?")
    output_lines.append(f"   Formula: Commission Fee + Delivery Fee - Payment Processing Fee - Refunds/Chargebacks")
    output_lines.append(f"   -> Total Platform Net Revenue: ${res6['total_platform_net_revenue']:,.2f}")
    output_lines.append(f"      (Calculated across {res6['included_orders']} orders with complete financial components; "
                        f"{res6['excluded_orders']} orders excluded due to missing components)")

    output_lines.append("\n" + "=" * 70)
    output_lines.append("END OF BUSINESS ANALYSIS REPORT")
    output_lines.append("=" * 70)

    cursor.close()
    conn.close()

    report_content = "\n".join(output_lines)

    # Save to disk
    Path(SQL_ANALYSIS_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(SQL_ANALYSIS_PATH, "w", encoding="utf-8") as f:
        f.write(report_content)
    print(f"[SQL ANALYSIS] Results exported to: {SQL_ANALYSIS_PATH}")

    return report_content


if __name__ == "__main__":
    report = run_business_analysis()
    print("\n" + report)
