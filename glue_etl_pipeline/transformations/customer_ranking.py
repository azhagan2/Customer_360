# glue_etl_pipeline/transformations/customer_ranking.py
def transform_top_customers_sql(spark):
    return spark.sql("""
        WITH customer_spending AS (
            SELECT customer_id, SUM(total_amount) AS total_spent,
                   COUNT(order_id) AS total_orders, MAX(order_date) AS last_purchase_date
            FROM orders
            WHERE order_date >= date_add(current_date(), -365)
            GROUP BY customer_id
        ),
        customer_ranking AS (
            SELECT c.country, c.customer_id,
                   CONCAT(c.first_name, ' ', c.last_name) AS full_name,
                   c.email, cs.total_spent, cs.total_orders,
                   cs.last_purchase_date,
                   RANK() OVER (PARTITION BY c.country ORDER BY cs.total_spent DESC) AS spending_rank
            FROM customer_spending cs
            JOIN customers c ON cs.customer_id = c.customer_id
        )
        SELECT * FROM customer_ranking
        WHERE spending_rank <= 10 AND country LIKE 'United %'
    """)
