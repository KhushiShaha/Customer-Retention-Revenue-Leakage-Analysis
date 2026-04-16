import os
import pandas as pd


def load_data():
    orders = pd.read_csv("data/olist_orders_dataset.csv")
    customers = pd.read_csv("data/olist_customers_dataset.csv")
    order_items = pd.read_csv("data/olist_order_items_dataset.csv")
    products = pd.read_csv("data/olist_products_dataset.csv")

    print("Orders shape:", orders.shape)
    print("Customers shape:", customers.shape)
    print("Order items shape:", order_items.shape)
    print("Products shape:", products.shape)

    return orders, customers, order_items, products


def create_base_table(orders, customers, order_items):
    base = orders.merge(customers, on="customer_id", how="left")
    base = base.merge(order_items, on="order_id", how="left")

    print("\nBase table shape:", base.shape)
    return base


def add_revenue(base):
    base["revenue"] = base["price"] + base["freight_value"]
    return base


def create_order_summary(base):
    order_summary = base.groupby(
        ["order_id", "customer_unique_id", "order_purchase_timestamp"],
        as_index=False
    )["revenue"].sum()

    print("\nOrder summary shape:", order_summary.shape)
    return order_summary


def create_customer_summary(order_summary):
    customer_summary = order_summary.groupby(
        "customer_unique_id",
        as_index=False
    ).agg({
        "order_id": "nunique",
        "revenue": "sum"
    })

    customer_summary.columns = ["customer_unique_id", "order_count", "total_revenue"]

    print("\nCustomer summary shape:", customer_summary.shape)
    return customer_summary


def classify_customers(customer_summary):
    customer_summary["customer_type"] = customer_summary["order_count"].apply(
        lambda x: "Repeat" if x > 1 else "One-time"
    )

    distribution = customer_summary["customer_type"].value_counts(normalize=True) * 100

    print("\nCustomer type distribution:")
    print(customer_summary["customer_type"].value_counts())

    print("\nCustomer type percentage:")
    print(distribution)

    return customer_summary


def calculate_monthly_revenue(order_summary):
    order_summary["order_purchase_timestamp"] = pd.to_datetime(order_summary["order_purchase_timestamp"])
    order_summary["order_month"] = order_summary["order_purchase_timestamp"].dt.to_period("M").astype(str)

    monthly_revenue = order_summary.groupby("order_month", as_index=False)["revenue"].sum()

    return order_summary, monthly_revenue


def cohort_analysis(order_summary):
    order_summary["purchase_month"] = order_summary["order_purchase_timestamp"].dt.to_period("M")

    cohort = order_summary.groupby("customer_unique_id")["purchase_month"].min().reset_index()
    cohort.columns = ["customer_unique_id", "cohort_month"]

    order_summary = order_summary.merge(cohort, on="customer_unique_id", how="left")

    order_summary["cohort_index"] = (
        (order_summary["purchase_month"].dt.year - order_summary["cohort_month"].dt.year) * 12
        + (order_summary["purchase_month"].dt.month - order_summary["cohort_month"].dt.month)
    )

    cohort_data = order_summary.groupby(
        ["cohort_month", "cohort_index"]
    )["customer_unique_id"].nunique().reset_index()

    cohort_pivot = cohort_data.pivot(
        index="cohort_month",
        columns="cohort_index",
        values="customer_unique_id"
    )

    cohort_size = cohort_pivot.iloc[:, 0]
    retention_matrix = cohort_pivot.divide(cohort_size, axis=0)

    return retention_matrix, order_summary


def identify_leakage(order_summary, customer_summary):
    last_date = order_summary["order_purchase_timestamp"].max()

    last_purchase = order_summary.groupby(
        "customer_unique_id",
        as_index=False
    )["order_purchase_timestamp"].max()

    last_purchase["days_since_last_purchase"] = (
        last_date - last_purchase["order_purchase_timestamp"]
    ).dt.days

    customer_leakage = customer_summary.merge(
        last_purchase,
        on="customer_unique_id",
        how="left"
    )

    threshold = customer_leakage["total_revenue"].quantile(0.80)

    high_risk = customer_leakage[
        (customer_leakage["total_revenue"] >= threshold) &
        (customer_leakage["days_since_last_purchase"] > 90)
    ]

    print("\nHigh-risk customers count:", high_risk.shape[0])

    return customer_leakage, high_risk, threshold


def add_recommendations(customer_leakage, threshold):
    def recommend(row):
        if row["total_revenue"] >= threshold and row["days_since_last_purchase"] > 90:
            return "Target with retention campaign"
        elif row["days_since_last_purchase"] > 90:
            return "Re-engage with offer"
        else:
            return "Maintain engagement"

    customer_leakage["recommendation"] = customer_leakage.apply(recommend, axis=1)
    return customer_leakage


def export_outputs(monthly_revenue, customer_summary, high_risk, customer_leakage, retention_matrix):
    os.makedirs("outputs", exist_ok=True)

    monthly_revenue.to_csv("outputs/monthly_revenue.csv", index=False)
    customer_summary.to_csv("outputs/customer_summary.csv", index=False)
    high_risk.to_csv("outputs/high_risk_customers.csv", index=False)
    customer_leakage.to_csv("outputs/customer_recommendations.csv", index=False)
    retention_matrix.to_csv("outputs/retention_matrix.csv")


if __name__ == "__main__":
    orders, customers, order_items, products = load_data()

    base = create_base_table(orders, customers, order_items)
    base = add_revenue(base)

    order_summary = create_order_summary(base)
    customer_summary = create_customer_summary(order_summary)
    customer_summary = classify_customers(customer_summary)

    order_summary, monthly_revenue = calculate_monthly_revenue(order_summary)
    retention_matrix, order_summary = cohort_analysis(order_summary)

    customer_leakage, high_risk, threshold = identify_leakage(order_summary, customer_summary)
    customer_leakage = add_recommendations(customer_leakage, threshold)

    export_outputs(monthly_revenue, customer_summary, high_risk, customer_leakage, retention_matrix)

    print("\nAll output files saved in 'outputs/' folder.")