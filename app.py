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
        # Run the async function and get the final results
        results, duration = asyncio.run(main_scraper_func(input_df))
        # Put the final results into the result queue
        result_queue.put((results, duration))
    except Exception as e:
        # If there's an error, put the exception in the queue
        result_queue.put(e)

# --- UI: SIDEBAR FOR USER INPUTS ---
st.sidebar.header("📊 Control Panel")
search_term = st.sidebar.text_input("Enter a Product to Search:", "M.A.C Lipstick")
site_list = ["Nykaa", "Amazon", "Myntra"]
selected_sites = st.sidebar.multiselect(
    "Select Competitor Sites:", options=site_list, default=["Myntra", "Nykaa"]
)
pincodes_input = st.sidebar.text_area(
    "Enter Pincodes to Check (one per line):", "201301\n700020"
)

# --- Main Application Logic ---
st.title("🚚 Competitor Delivery Speed Checker")

if st.sidebar.button("🚀 Get Delivery Speeds"):
    if not search_term or not selected_sites or not pincodes_input:
        st.warning("Please provide a product, at least one site, and at least one pincode.")
    else:
        pincode_list = [p.strip() for p in pincodes_input.split('\n') if p.strip()]
        st.header(f"🔍 Results for: *{search_term}*")

        # --- TIME-BASED PROGRESS BAR IMPLEMENTATION ---
        TIME_ESTIMATES_PER_PINCODE = {
            "Amazon": 9,
            "Nykaa": 7,
            "Myntra": 5,
            "Default": 10
        }
        
        # --- CORRECTED CALCULATION FOR PARALLEL PROCESSING ---
        # 1. Find the maximum (slowest) time per pincode among the selected sites.
        if selected_sites:
            max_time_per_pincode = max(
                TIME_ESTIMATES_PER_PINCODE.get(site, TIME_ESTIMATES_PER_PINCODE["Default"])
                for site in selected_sites
            )
        else:
            max_time_per_pincode = 0

        # 2. Start with a fixed 15-second buffer for setup.
        total_estimated_time = 15
        
        # 3. Add the time for all pincodes, based on the single slowest site's estimate.
        total_estimated_time += max_time_per_pincode * len(pincode_list)
        # --- END OF CORRECTION ---

        result_queue = queue.Queue()
        
        input_data_list = [{'site_name': site, 'style_name': search_term, 'pincode': p} for site in selected_sites for p in pincode_list]
        input_df = pd.DataFrame(input_data_list)

        scraper_thread = threading.Thread(
            target=run_scraper_in_thread,
            args=(input_df, result_queue)
        )
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

        # --- DATA PROCESSING AND DISPLAY ---
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
            
            # --- MODIFIED: PINCODES NOT SERVICEABLE ---
            st.subheader("🚫 Pincodes Not Serviceable by Site")
            not_serviceable_df = display_df[display_df['Days to Delivery'].isna()]
            if not not_serviceable_df.empty:
                unserviceable_counts = not_serviceable_df.groupby('Site')['Pincode'].nunique().reset_index()
                unserviceable_counts.rename(columns={'Pincode': 'Non-Serviceable Pincode Count'}, inplace=True)
                
                # MODIFICATION: Place the dataframe inside a column to constrain its width
                col1, col2 = st.columns([1, 2]) # Create two columns, the first being narrower
                with col1:
                    st.dataframe(unserviceable_counts)
            else:
                st.success("✅ All selected pincodes appear to be serviceable by all sites!")
            
            st.markdown("---")

            # --- DETAILED COMPARISON TABLE ---
            st.subheader("🚚 Detailed Delivery Speed Comparison (in Days)")
            pivoted_df = display_df.pivot_table(index='Pincode', columns='Site', values='Days to Delivery').reindex(columns=selected_sites)
            st.dataframe(style_comparison_df(pivoted_df, selected_sites), width='stretch')

            with st.expander("Show All Processed Data"):
                st.dataframe(display_df)
else:
    st.info("Select a product, sites, and pincodes from the sidebar, then click 'Get Delivery Speeds' to start.")

