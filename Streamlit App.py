import streamlit as st
import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

db_config = {
    'user': 'vasu',
    'password': 'V@suM@v@ni5',
    'host': '127.0.0.1',
    'database': 'commodities'
}

# Database connection function
def get_crops():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT crop FROM croptypes")
        crops = ["Select a crop"] + [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return crops
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return []

def get_continents():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT Continent FROM continentcountry")
        continents = ["Select a continent"] + [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return continents
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return []

def get_countries_by_continent(continent):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = "SELECT Country FROM continentcountry WHERE Continent = %s"
        cursor.execute(query, (continent,))
        countries = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return countries
    except mysql.connector.Error as err:
        st.error(f"Error: {err}")
        return []

# Main app
def main():
    st.title("Crop Arrival Analysis")

    # Sidebar
    st.sidebar.header("Select filters")
    crops = get_crops()
    continents = get_continents()

    selected_crop = st.sidebar.selectbox("Choose a crop:", crops)
    selected_continent = st.sidebar.selectbox("Choose a continent:", continents)
    if st.sidebar.button("Submit"):
        if selected_crop == "Select a crop":
            st.error("Please select a valid crop before clicking Submit.")
        else:
            with st.spinner("Processing data..."):
                try:
                    # Connect to the database
                    conn = mysql.connector.connect(**db_config)
                    cursor = conn.cursor()

                    # Query: Grain Type and Subtype
                    type_subtype_query = """
                    SELECT grain_type, grain_subtype
                    FROM croptypes
                    WHERE crop = %s
                    """
                    cursor.execute(type_subtype_query, (selected_crop,))
                    type_subtype_result = cursor.fetchone()
                    grain_type = type_subtype_result[0] if type_subtype_result else "N/A"
                    grain_subtype = type_subtype_result[1] if type_subtype_result else "N/A"

                    # Query: Import and Export Summary
                    import_export_query = """
                    SELECT SUM(import_quantity) AS total_import,
                           SUM(export_quantity) AS total_export,
                           SUM(import_usd) AS total_import_usd,
                           SUM(export_usd) AS total_export_usd
                    FROM importexport
                    WHERE crop = %s
                    """
                    cursor.execute(import_export_query, (selected_crop,))
                    import_export_result = cursor.fetchone()
                    total_import = import_export_result[0] if import_export_result[0] else 0
                    total_export = import_export_result[1] if import_export_result[1] else 0
                    total_import_usd = import_export_result[2] if import_export_result[2] else 0
                    total_export_usd = import_export_result[3] if import_export_result[3] else 0

                    # Query: Price and Arrival Summary
                    price_arrival_query = """
                    SELECT SUM(price * arrival) / SUM(arrival) AS average_price,
                           SUM(arrival) AS total_arrival
                    FROM pricearrivals
                    WHERE crop = %s
                    """
                    cursor.execute(price_arrival_query, (selected_crop,))
                    price_arrival_result = cursor.fetchone()
                    average_price = price_arrival_result[0] if price_arrival_result[0] else 0
                    total_arrival = price_arrival_result[1] if price_arrival_result[1] else 0

                    # Query for time-series data: Export, Import, and Arrivals
                    time_series_query = """
                    SELECT DATE_FORMAT(Month, '%b-%y') AS formatted_date, 
                        SUM(import_quantity) AS import_quantity, 
                        SUM(export_quantity) AS export_quantity, 
                        SUM(import_usd) AS import_usd, 
                        SUM(export_usd) AS export_usd,
                        SUM(arrival) AS arrivals
                    FROM (
                        SELECT Month, import_quantity, export_quantity, import_usd, export_usd, 0 AS arrival
                        FROM importexport
                        WHERE crop = %s
                        UNION ALL
                        SELECT Date AS Month, 0 AS import_quantity, 0 AS export_quantity, 0 AS import_usd, 0 AS export_usd, arrival
                        FROM pricearrivals
                        WHERE crop = %s
                    ) AS combined_data
                    GROUP BY Month
                    ORDER BY STR_TO_DATE(Month, '%Y-%m')
                    """
                    cursor.execute(time_series_query, (selected_crop, selected_crop))
                    time_series_data = cursor.fetchall()

                    cursor.close()
                    conn.close()

                    # Process time-series data
                    df = pd.DataFrame(
                        time_series_data,
                        columns=[
                            "Formatted Date",
                            "Import Quantity",
                            "Export Quantity",
                            "Import USD",
                            "Export USD",
                            "Arrivals",
                        ],
                    )
                    df["Formatted Date"] = pd.to_datetime(df["Formatted Date"], format='%b-%y')
                    df = df.sort_values("Formatted Date")

                    # Sidebar summary details
                    st.sidebar.title(f"Summary for {selected_crop}")
                    st.sidebar.subheader("Grain Details")
                    st.sidebar.write(f"**Grain Type:** {grain_type}")
                    st.sidebar.write(f"**Grain Subtype:** {grain_subtype}")

                    st.sidebar.subheader("Import and Export Summary")
                    st.sidebar.write(f"**Total Import (Quantity):** {total_import}")
                    st.sidebar.write(f"**Total Export (Quantity):** {total_export}")
                    st.sidebar.write(f"**Total Import (Million USD):** ${total_import_usd:.2f}")
                    st.sidebar.write(f"**Total Export (Million USD):** ${total_export_usd:.2f}")

                    st.sidebar.subheader("Price and Arrival Summary")
                    st.sidebar.write(f"**Average Price:** ${average_price:.2f}")
                    st.sidebar.write(f"**Total Arrivals (Tonnes):** {total_arrival:.2f}")

                    # Plotting metrics
                    metrics = [
                        ("Export Quantity", "Export Quantity", "blue"),
                        ("Export USD", "Export USD ($)", "green"),
                        ("Import Quantity", "Import Quantity", "red"),
                        ("Import USD", "Import USD ($)", "orange"),
                        ("Arrivals", "Arrivals (Tonnes)", "purple"),
                    ]

                    # Create 3 rows with 2 plots in each row
                    rows = [st.columns(2) for _ in range(3)]
                    plots = []

                    for idx, (metric, ylabel, color) in enumerate(metrics):
                        fig, ax = plt.subplots(figsize=(5, 3))
                        ax.plot(df["Formatted Date"], df[metric], label=metric, color=color)
                        ax.set_title(metric)
                        ax.set_xlabel("Date")
                        ax.set_ylabel(ylabel)
                        ax.legend()

                        # Set xticks with a gap of 1
                        xticks = df["Formatted Date"].iloc[::4]
                        ax.set_xticks(xticks)
                        ax.set_xticklabels(xticks.dt.strftime('%b-%y'), rotation=90, ha="right")
                        
                        plots.append(fig)

                        # Display each plot in its respective cell
                        row_idx, col_idx = divmod(idx, 2)
                        rows[row_idx][col_idx].pyplot(fig)

                # Extra functionality: Show continent-wise summary
                    if selected_continent != "Select a continent":
                        countries = get_countries_by_continent(selected_continent)
                        if countries:
                            placeholders = ", ".join(f"'{country}'" for country in countries)
                            conn = mysql.connector.connect(**db_config)
                            cursor = conn.cursor()
                            
                            continent_query = f"""
                            SELECT Country, 
                                SUM(import_quantity) AS total_import,
                                SUM(export_quantity) AS total_export,
                                SUM(import_usd) AS total_import_usd,
                                SUM(export_usd) AS total_export_usd,
                                SUM(price * arrival) / SUM(arrival) AS average_price,
                                SUM(arrival) AS total_arrival
                            FROM importexport
                            JOIN pricearrivals ON importexport.crop = pricearrivals.crop
                            WHERE importexport.crop = '{selected_crop}' AND Country IN ({placeholders})
                            GROUP BY Country
                            """
                            cursor.execute(
                                continent_query
                            )
                            continent_data = cursor.fetchall()
                            print(continent_data)
                            continent_df = pd.DataFrame(
                                continent_data,
                                columns=[
                                    "Country",
                                    "Total Import",
                                    "Total Export",
                                    "Total Import (USD)",
                                    "Total Export (USD)",
                                    "Average Price",
                                    "Total Arrivals",
                                ],
                            )
                            st.subheader(f"Country-Wise Summary for {selected_continent}")
                            st.dataframe(continent_df)
                            cursor.close()
                            conn.close()


                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")

# Run the app
if __name__ == "__main__":
    main()
