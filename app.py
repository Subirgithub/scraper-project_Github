import asyncio
import streamlit as st
import pandas as pd
import numpy as np
import threading
import queue
import time
# Assuming your scraper file is named Scraper_del_check.py
from Scraper_del_check import main_scraper_func

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Competitor Delivery Speed Checker",
    page_icon="🚚",
    layout="wide"
)

# --- DATA FOR NEW INPUTS ---
# Data for hierarchical product selection
category_data = {
    'master_category': ['Accessories', 'Accessories', 'Accessories', 'Apparel', 'Apparel', 'Footwear', 'Footwear'],
    'business_unit': ['Baby Care & Toys', 'Electronics', 'Eyewear', 'International Brands', 'Kids Wear', "Men's Casual Footwear", 'Sports Footwear'],
    'Product type': ['Baby Gear & Nursery', 'Headphones', 'Sunglasses', 'Jeans', 'Dresses', 'Casual Shoes', 'Sports Shoes']
}
category_df = pd.DataFrame(category_data)

# Pre-configured pincodes for major cities
CITY_PINCODES = {
    "Agra": ["282001", "282005", "282007"],
    "Ahmedabad": ["380015", "380058", "380001"],
    "Allahabad": ["211002", "211003", "211001"],
    "Amritsar": ["143001", "143002", "143105"],
    "Anantapur": ["515001", "515004", "515002"],
    "Bangalore": ["560037", "560066", "560068"],
    "Bhopal": ["462001", "462030", "462016"],
    "Bhubaneswar": ["751024", "751003", "751002"],
    "Chandigarh": ["160036", "160047", "160022"],
    "Chennai": ["600100", "603103", "600119"],
    "Coimbatore": ["641035", "641004", "641006"],
    "Dehradun": ["248001", "248007", "248002"],
    "Delhi": ["201301", "110009", "122001"],
    "Eluru": ["534002", "534001", "534005"],
    "Ernakulam": ["682030", "682024", "682020"],
    "Gorakhpur": ["273001", "273015", "273008"],
    "Guntur": ["522001", "522002", "522006"],
    "Guwahati": ["781028", "781005", "781001"],
    "Hyderabad": ["500084", "500032", "500081"],
    "Indore": ["452001", "452010", "452016"],
    "Jaipur": ["302017", "302020", "302012"],
    "Jalandhar": ["144001", "144002", "144003"],
    "Jammu": ["180001", "180004", "180002"],
    "Jamshedpur": ["831001", "831012", "831005"],
    "Jodhpur": ["342001", "342008", "342006"],
    "Kanpur": ["208001", "208002", "208011"],
    "Kolkata": ["700156", "700026", "700135"],
    "Lucknow": ["226010", "226016", "226003"],
    "Ludhiana": ["141008", "141001", "142021"],
    "Meerut": ["250001", "250002", "250004"],
    "Moradabad": ["244001", "244412", "244102"],
    "Mumbai": ["401107", "410210", "400067"],
    "Mysore": ["570019", "570017", "570016"],
    "Nagpur": ["440024", "440013", "440010"],
    "Nashik": ["422003", "422009", "422101"],
    "Patna": ["800001", "800020", "801503"],
    "Pune": ["411057", "411014", "411045"],
    "Raipur": ["492001", "492015", "492013"],
    "Rajahmundry": ["533101", "533103", "533105"],
    "Rajkot": ["360005", "360001", "360004"],
    "Ranchi": ["834001", "834002", "834009"],
    "Srinagar": ["190001", "190015", "190005"],
    "Surat": ["395007", "395009", "395006"],
    "Trivandrum": ["695011", "695583", "695003"],
    "Udaipur": ["313001", "313002", "799120"],
    "Vadodara": ["390019", "390012", "390007"],
    "Varanasi": ["221005", "221001", "221010"],
    "Vijayawada": ["520007", "520010", "520001"],
    "Vishakhapatnam": ["530016", "530017", "530045"],
    "Vizianagaram": ["535002", "535001", "535128"]
}
# CITY_PINCODES = {
#     "Bangalore": ["560001", "560095", "560068"],
#     "Delhi": ["110001", "110006", "110017"],
#     "Mumbai": ["400001", "400013", "400050"]
# }

# --- STYLING FUNCTION (No changes needed) ---
def style_comparison_df(df, sites_to_style):
    valid_sites_to_style = [site for site in sites_to_style if site in df.columns]
    if not valid_sites_to_style:
        return df.style
    return df.style.highlight_min(subset=valid_sites_to_style, color='#C8E6C9', axis=1).highlight_null(color='#FFCDD2', subset=valid_sites_to_style).format("{:.0f}", na_rep="Unavailable", subset=valid_sites_to_style)

# --- THREADING FUNCTION ---
def run_scraper_in_thread(input_df, result_queue):
    """
    A wrapper function to run the async scraper in a separate thread.
    This prevents the Streamlit UI from freezing.
    """
    try:
        results, duration = asyncio.run(main_scraper_func(input_df))
        result_queue.put((results, duration))
    except Exception as e:
        result_queue.put(e)

# --- CSS to Increase Sidebar Width ---
st.markdown(
    """
    <style>
    [data-testid="stSidebar"] {
        width: 400px !important; # Set the width to your desired value
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# --- UI: SIDEBAR FOR USER INPUTS ---
st.sidebar.header("📊 Control Panel")
#search term logic
search_term = st.sidebar.text_input("Enter a Product to Search:", "M.A.C Lipstick")
# # --- NEW: HIERARCHICAL PRODUCT SELECTION ---
# st.sidebar.subheader("1. Select a Product")
# # Dropdown for Master Category
# master_cat_list = category_df['master_category'].unique()
# selected_master_cat = st.sidebar.selectbox("Master Category:", master_cat_list)

# # Dropdown for Business Unit (filtered by Master Category)
# bu_list = category_df[category_df['master_category'] == selected_master_cat]['business_unit'].unique()
# selected_bu = st.sidebar.selectbox("Business Unit:", bu_list)

# # Dropdown for Product Type (filtered by Business Unit)
# product_type_list = category_df[category_df['business_unit'] == selected_bu]['Product type'].unique()
# selected_product_type = st.sidebar.selectbox("Product Type:", product_type_list)
# # The selected_product_type will be our new search term
# search_term = selected_product_type
# # --- END OF NEW PRODUCT SELECTION ---

# --- Site selection remains the same ---
st.sidebar.subheader("2. Select Competitor Sites")
site_list = ["Nykaa", "Amazon", "Myntra"]
selected_sites = st.sidebar.multiselect(
    "Select Sites:", options=site_list, default=["Myntra", "Nykaa"]
)

# --- NEW: PINCODE OR CITY SELECTION ---
st.sidebar.subheader("3. Select Locations")
location_choice = st.sidebar.radio(
    "Choose location input method:",
    ("Enter Pincodes Manually", "Select by City")
)

pincode_list = []
if location_choice == "Enter Pincodes Manually":
    pincodes_input = st.sidebar.text_area(
        "Enter Pincodes to Check (one per line):", "201301\n700020"
    )
else:
    selected_cities = st.sidebar.multiselect(
        "Select Cities:", options=list(CITY_PINCODES.keys()), default=["Bangalore", "Delhi"]
    )
# --- END OF NEW LOCATION SELECTION ---


# --- Main Application Logic ---
st.title("🚚 Competitor Delivery Speed Checker")

if st.sidebar.button("🚀 Get Delivery Speeds"):
    # --- UPDATE: Logic to build pincode list based on user choice ---
    if location_choice == "Enter Pincodes Manually":
        if pincodes_input:
            pincode_list = [p.strip() for p in pincodes_input.split('\n') if p.strip()]
    else: # Select by City
        if selected_cities:
            # Flatten the list of lists and remove duplicates
            pincode_set = set()
            for city in selected_cities:
                pincode_set.update(CITY_PINCODES[city])
            pincode_list = sorted(list(pincode_set))

    # --- UPDATE: Validation for new inputs ---
    if not search_term or not selected_sites or not pincode_list:
        st.warning("Please select a product, at least one site, and at least one location.")
    else:
        st.header(f"🔍 Results for: *{search_term}*")

        # --- TIME-BASED PROGRESS BAR IMPLEMENTATION (No changes needed here) ---
        TIME_ESTIMATES_PER_PINCODE = {"Amazon": 9, "Nykaa": 7, "Myntra": 5, "Default": 10}
        
        if selected_sites:
            max_time_per_pincode = max(
                TIME_ESTIMATES_PER_PINCODE.get(site, TIME_ESTIMATES_PER_PINCODE["Default"])
                for site in selected_sites
            )
        else:
            max_time_per_pincode = 0

        total_estimated_time = 15 + (max_time_per_pincode * len(pincode_list))

        result_queue = queue.Queue()
        input_data_list = [{'site_name': site, 'style_name': search_term, 'pincode': p} for site in selected_sites for p in pincode_list]
        input_df = pd.DataFrame(input_data_list)

        scraper_thread = threading.Thread(target=run_scraper_in_thread, args=(input_df, result_queue))
        scraper_thread.start()

        st.subheader("📊 Estimated Scraping Progress")
        progress_bar = st.progress(0)
        status_text = st.empty()
        start_time = time.time()

        while scraper_thread.is_alive():
            elapsed_time = time.time() - start_time
            estimated_progress = int((elapsed_time / total_estimated_time) * 100)
            progress = min(estimated_progress, 99)
            progress_bar.progress(progress)
            status_text.text(f"Estimated progress: {progress}%...")
            time.sleep(1)

        progress_bar.progress(100)
        
        result_payload = result_queue.get()
        
        if isinstance(result_payload, Exception):
            status_text.error("An error occurred during scraping.")
            st.exception(result_payload)
            st.stop()
        
        results_df, execution_time = result_payload
        minutes, seconds = divmod(execution_time, 60)
        status_text.success(f"Scraping complete in {int(minutes)}m {int(seconds)}s!")
        # --- END OF PROGRESS BAR IMPLEMENTATION ---

        # --- DATA PROCESSING AND DISPLAY (No changes needed here) ---
        display_df = results_df.rename(columns={'site_name': 'Site', 'pincode': 'Pincode', 'days_to_delivery': 'Days to Delivery'})
        valid_results = display_df.dropna(subset=['Days to Delivery'])

        if results_df.empty:
            st.error("Scraper returned no data. Please check the scraper logs.")
        else:
            st.markdown("---")
            st.subheader("📊 Average Delivery Speed by Site")
            if not valid_results.empty:
                avg_delivery_days = valid_results.groupby('Site')['Days to Delivery'].mean().round(1)
                best_avg_site = avg_delivery_days.idxmin()
                avg_cols = st.columns(len(avg_delivery_days))
                for i, (site, avg_days) in enumerate(avg_delivery_days.items()):
                    delta_text = "🏆 Fastest" if site == best_avg_site else None
                    avg_cols[i].metric(label=f"Avg. Speed: {site}", value=f"{avg_days} Days", delta=delta_text, delta_color="inverse")
            else:
                st.info("No data to calculate average delivery speeds.")

            st.markdown("---")
            
            st.subheader("🚫 Pincodes Not Serviceable by Site")
            not_serviceable_df = display_df[display_df['Days to Delivery'].isna()]
            if not not_serviceable_df.empty:
                unserviceable_counts = not_serviceable_df.groupby('Site')['Pincode'].nunique().reset_index()
                unserviceable_counts.rename(columns={'Pincode': 'Non-Serviceable Pincode Count'}, inplace=True)
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.dataframe(unserviceable_counts)
            else:
                st.success("✅ All selected pincodes appear to be serviceable by all sites!")
            
            st.markdown("---")

            st.subheader("🚚 Detailed Delivery Speed Comparison (in Days)")
            # Create a dictionary to map any internal variants back to the main display name.
            site_name_map = {
                'Nykaafashion': 'Nykaa',  # Maps the scraped name to the display name
                'Nykaa': 'Nykaa',        # Keeps the original Nykaa entries as 'Nykaa'
                # Add other variants if they exist (e.g., 'AmazonIn': 'Amazon')
            }

            # Apply the mapping to the 'Site' column in your DataFrame# This ensures that all 'Nykaafashion' entries are now labeled 'Nykaa'.
            display_df['Site'] = display_df['Site'].replace(site_name_map)
            pivoted_df = display_df.pivot_table(index='Pincode', columns='Site', values='Days to Delivery').reindex(columns=selected_sites)
            st.dataframe(style_comparison_df(pivoted_df, selected_sites), width='stretch')

            with st.expander("Show All Processed Data"):
                st.dataframe(display_df)
else:
    st.info("Select a product, sites, and locations from the sidebar, then click 'Get Delivery Speeds' to start.")