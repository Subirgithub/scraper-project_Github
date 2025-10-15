#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#from playwright.async_api import async_playwright
#from playwright_stealth import stealth_async
#from playwright_stealth.stealth import stealth_sync, stealth_async


import traceback
import asyncio
import pandas as pd
import io
import re
import time
from datetime import date,datetime,timedelta
from playwright.async_api import async_playwright, TimeoutError, expect
import traceback
import os
import random
#os.chdir('/Users/subir.paul2/Desktop/Work/Myntra/Crawl')
#print("Current Working Directory:", os.getcwd())

async def human_like_scroll(page):
    """Scrolls the page to mimic human behavior."""
    print("--- Scrolling page to appear more human...")
    await page.evaluate("window.scrollTo(0, 0)")
    await page.wait_for_timeout(random.uniform(500, 1000))
    # Scroll down a random amount
    scroll_amount = random.randint(300, 600)
    await page.mouse.wheel(0, scroll_amount)
    await page.wait_for_timeout(random.uniform(1000, 2500))

    # --- THIS LINE IS NOW FIXED ---
    # Scroll back up a little by a random amount
    scroll_amount = random.randint(-250, -100) 
    # --- END OF FIX ---

    await page.mouse.wheel(0, scroll_amount)
    await page.wait_for_timeout(random.uniform(500, 1000))


async def check_and_close_intermittent_popup(page):
    """Looks for and closes the 'Push Notifications' pop-up."""
    # This is the correct selector for the "No thanks" button.
    no_thanks_selector = "#wzrk-cancel" 

    try:
        # Use a short timeout as this pop-up is intermittent
        await page.locator(no_thanks_selector).click(timeout=2000)
        print("--- Closed 'Push Notifications' pop-up. ---")
        await page.wait_for_timeout(1000) # Wait a moment for it to disappear
    except Exception:
        pass # This is normal, it just means no pop-up was found        


# --- CONFIGURATION: UPDATE SELECTORS HERE WHEN THEY BREAK ---
SITE_CONFIG = {
    "Amazon": {
        "initial_popup_close_selector": None,
        "pre_pincode_click_selector": "#contextualIngressPtLabel_deliveryShortLine",
        "pincode_container_selector": None,
        "pincode_input_selector": "#GLUXZipUpdateInput",
        "pincode_submit_selector": "input[aria-labelledby='GLUXZipUpdate-announce']",
        # --- ADD THIS NEW KEY ---
        "unavailable_selector": "#availability span:has-text('Currently unavailable.')",
       # "unavailable_selector": "#availability span:has-text('Currently unavailable.'), span.a-color-price:has-text('Currently unavailable.')",
        "delivery_info_selectors": [
        # --- ADD THIS NEW ENTRY AT THE TOP ---
        {
            "type": "unserviceable",
            "selector": "//span[contains(text(), 'cannot be shipped to your selected delivery location')]"
        },
        # --- Your existing selectors go here ---
        {
            "type": "primary_delivery",
            "selector": "span[data-csa-c-content-id='DEXUnifiedCXPDM']"
        },
        {
            "type": "secondary_info",
            "selector": "span[data-csa-c-content-id='DEXUnifiedCXSDM']"
        }
    ]
},
    "Flipkart": {
    "initial_popup_close_selector": "button._2KpZ6l._2doB4z",
    # This selector is for the pre-selected pincode div that needs to be clicked
    "pre_pincode_click_selector": "div.JqZtEs",
    "pincode_input_selector": "input[placeholder='Enter delivery pincode']",
    "pincode_submit_selector": "//span[text()='Check']",
    "delivery_info_selector": "div.hVvnXm"
},
    "Myntra": {
    "initial_popup_close_selector": None,
    # CORRECTED: This selector targets the entire delivery options area.
    # Clicking it is the first step required to enable the input box.
    "pre_pincode_click_selector": "button.pincode-check-another-pincode",
    
    # This selector is correct for the input box itself.
    "pincode_input_selector": "input[placeholder='Enter pincode']",
    
    # This selector for the "Check" button is correct.
    "pincode_submit_selector": "input[value='Check']",
    
    # Add an unavailable selector for products that are out of stock.
    "unavailable_selector": "div.pdp-out-of-stock",
    
    # This structured format is more flexible and matches the delivery info HTML.
    "delivery_info_selectors": [
        {
            "type": "unserviceable",
            "selector": "p.pincode-error"
        },
        {
            "type": "primary_delivery",
            "selector": "h4.pincode-serviceabilityTitle"
        }
    ]
},
    # In your SITE_CONFIG dictionary
    "Nykaa": {
    "initial_popup_close_selector": None,
    "pre_pincode_click_selector": "//button[text()='Change']",
    "pincode_input_selector": "input[placeholder='Enter pincode']",
    "pincode_submit_selector": "//button[text()='Check']",
    "unavailable_selector": "//button[normalize-space()='Notify Me']",    
   # "delivery_info_selector": "//span[contains(text(), 'Delivery by')]"
    "delivery_info_selectors": [
        {
            "type": "unserviceable", 
            "selector": "//span[contains(text(), 'Does not ship to pincode')]"
        },
        {
            "type": "primary_delivery", 
            "selector": "//span[contains(text(), 'Delivery by')]"
        },
        {
            "type": "secondary_info", 
            "selector": "//span[contains(text(), 'COD available')]"
        }
    ]
        
},
    "Nykaafashion": {
    "initial_popup_close_selector": None,
    "pre_pincode_click_selector": "//button[text()='Edit']",
    "pincode_input_selector": "[data-at='pincode-input']",
    # This XPath selector is more reliable than a CSS class.
    "pincode_submit_selector": "//button[text()='Apply']",
    # This selector is also more robust.
    "unavailable_selector": "//button[normalize-space()='Notify Me']",    
    "delivery_info_selector": "//h3[contains(text(), 'Delivery by')]"
},

    "Ajio": {
        "initial_popup_close_selector": None, # No known pop-up
        "pre_pincode_click_selector": '//div[@aria-label="Enter Pin-code To Know Estimated Delivery Date"]',
        "pincode_input_selector": "//input[@name='pincode']",
        "pincode_submit_selector": "//button[text()='CONFIRM PINCODE']",
        "delivery_info_selector": "//div[contains(@class, 'edd-message-container')]//span"
},
    "Meesho": {
        "initial_popup_close_selector": None,
        "pre_pincode_click_selector": None,
        "pincode_container_selector": None,
        "pincode_input_selector": "#pin", # Using the stable ID attribute
        "pincode_submit_selector": "//span[text()='CHECK']", # Finding the span by its text
        "delivery_info_selector": "//span[contains(text(), 'Delivery by')]" # Finding by partial text
    }
}


# In your Scraper_del_check.py file, replace the existing function with this one.

def extract_delivery_date(row):
    """
    Parses a date from various text formats, now including the
    "Day, Month DD" format (e.g., "Fri, Oct 17").
    """
    delivery_text = str(row['delivery_info'])
    scrape_date = row['scrape_date']

    # --- Keep existing logic for "same day", "next day" (No changes here) ---
    if re.search(r'in 2 hrs|same day|today', delivery_text, re.IGNORECASE):
        return scrape_date
    elif re.search(r'next day|tomorrow', delivery_text, re.IGNORECASE):
        return scrape_date + timedelta(days=1)

    # --- START OF MODIFICATION ---
    # This pattern will match "Jan", "January", "Feb", "February", etc.
    month_pattern = r"(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
    
    # NEW PATTERN: First, check for the "Month Day" format (e.g., "Oct 17")
    new_date_pattern = month_pattern + r"\s+(\d{1,2})"
    match = re.search(new_date_pattern, delivery_text, re.IGNORECASE)

    if match:
        # If the new pattern matches, extract the month and day
        month_str = match.group(1)
        day = int(match.group(2))
    else:
        # FALLBACK: If not, check for the original "Day Month" format (e.g., "17 Oct")
        original_date_pattern = r"(\d{1,2})\s+" + month_pattern
        match = re.search(original_date_pattern, delivery_text, re.IGNORECASE)
        if not match:
            # If neither pattern matches, return a null date
            return pd.NaT
        # If the original pattern matches, extract the day and month
        day = int(match.group(1))
        month_str = match.group(2)
    # --- END OF MODIFICATION ---

    # --- This logic remains the same. It builds the date from the extracted parts. ---
    try:
        date_str = f"{day} {month_str} {scrape_date.year}"
        delivery_dt = datetime.strptime(date_str, '%d %B %Y')
    except ValueError:
        try:
            delivery_dt = datetime.strptime(date_str, '%d %b %Y')
        except ValueError:
            return pd.NaT

    # Year Crossover Logic
    if delivery_dt.date() < scrape_date.date():
        delivery_dt = delivery_dt.replace(year=scrape_date.year + 1)

    return delivery_dt


USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/117.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
]

#Nykaa Specific functions
async def scrape_pincode_on_page_nykaa(page, site, pincode):
    """
    Attempts to enter a pincode using a deliberate, multi-step process to handle dynamic elements.
    """
    #print("entered function")
    config = SITE_CONFIG.get(site)
    if not config:
        return {"primary": "Site not configured", "secondary": ""}

    # MODIFIED: Loop for up to 2 attempts (1 initial + 1 retries)
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            # On retries (attempt > 0), the page is already reloaded by the except block.
            if attempt > 0:
                print(f"--- Starting Retry Attempt {attempt + 1}/{max_attempts} for pincode {pincode} ---")

            pre_click_selector = config.get("pre_pincode_click_selector")
            if pre_click_selector:
                try:
                    # Use a short timeout as this button may not be present on the first run.
                    await page.locator(pre_click_selector).first.click(timeout=3000)
                    #print("--- Clicked 'Change' button to enter a new pincode. ---")
                except Exception:
                    # This is expected if it's the first pincode check for this URL.
                    #print("--- No 'Change' button found, proceeding directly. ---")
                    pass

            # --- START OF MODIFICATION ---
            # STEP 2: Enter the pincode by typing character by character.
            pincode_input_element = page.locator(config["pincode_input_selector"]).first

            # --- ADD THIS LINE ---
            # Explicitly wait for the input box to be visible before interacting.
            #print("--- Waiting for pincode input to be visible... ---")
            await pincode_input_element.wait_for(state="visible", timeout=7000)

            #print("--- Pincode input is ready. Typing now. ---")
            await pincode_input_element.clear()

            await pincode_input_element.fill(pincode)
            await page.wait_for_timeout(500)

           # for char in pincode:
           #     await pincode_input_element.press(char, delay=random.randint(80, 250))
            # --- END OF MODIFICATION ---

            # pincode_input_element = page.locator(config["pincode_input_selector"]).first
            # pincode_container_selector = config.get("pincode_container_selector")

            # if pincode_container_selector:
            #     await page.locator(pincode_container_selector).first.click()

            # await pincode_input_element.hover()
            # await pincode_input_element.clear()
            # for char in pincode:
            #     await pincode_input_element.press(char, delay=random.randint(80, 250))

            #await page.wait_for_timeout(500)

            # --- STEP 2: Click the submit/check button ---
            if config.get("pincode_submit_selector"):
                await page.locator(config["pincode_submit_selector"]).first.click()

            # --- Data Extraction ---
            # Initialize a dictionary to hold the results.
            results = {"primary": "Not found", "secondary": ""}
            
            # Get the prioritized list of selectors from your config.
            delivery_selectors = config.get("delivery_info_selectors", [])
            
            # Loop through each selector in the order they are listed.
            for item in delivery_selectors:
                item_type = item["type"]
                selector = item["selector"]
                
                try:
                    # Use a short timeout to quickly check if the element is visible.
                    element = page.locator(selector).first
                    await element.wait_for(state="visible", timeout=5000)
                    
                    # If the element is found, grab its text.
                    text_content = (await element.inner_text()).strip()
                    
                    if item_type == "primary_delivery":
                        results["primary"] = text_content
                        
                    elif item_type == "unserviceable":
                        results["primary"] = text_content
                        # This is a final status. We can stop checking.
                        break
                        
                    elif item_type == "secondary_info":
                        results["secondary"] = text_content
                
                except Exception:
                    # It's normal for a selector not to be found. Just move to the next one.
                    pass

            # If we reach here, the attempt was successful.
            return results

        except Exception as e:
            # MODIFIED: Handle failure and decide whether to retry or fail permanently.
            print(f"--- Attempt {attempt + 1} for pincode {pincode} failed: {type(e).__name__} ---")
            if attempt < max_attempts - 1: # If this wasn't the last attempt
                print(f"--- Refreshing page and preparing for retry... ---")
                await page.reload(wait_until="domcontentloaded", timeout=60000)
            else: # This was the final attempt
                print(f"--- Final attempt for pincode {pincode} also failed.")
                return {"primary": f"Error: Failed after {max_attempts} attempts", "secondary": ""}

    # This is reached if all attempts in the loop fail
    return {"primary": "Error: All retry attempts failed.", "secondary": ""}

async def search_and_scrape_nykaa(page, site, search_term, pincode_group):   
    """
    Searches for a product on Nykaa, clicks the first result, 
    and then scrapes delivery speeds for a group of pincodes.
    """
    print(f"--- Searching for '{search_term}' on Nykaa... ---")

    # 1. Navigate to the homepage
   # await page.goto("https://www.nykaa.com/", wait_until="domcontentloaded")
    await page.goto("https://www.nykaa.com/skin/masks/c/8399/", wait_until="domcontentloaded", timeout=60000)
    #await page.goto("https://www.nykaa.com/skin/masks/c/8399/", wait_until="networkidle", timeout=60000)

    # 2. Find and click the search bar to ensure it's focused
    search_bar = page.locator('input[placeholder="Search on Nykaa"]')
    await search_bar.click()

    # ADD a deliberate pause AFTER clicking but BEFORE typing.
    # This gives the website's JavaScript time to prepare for input.
    print("--- Pausing after click to let the search bar initialize... ---")
    await page.wait_for_timeout(1000)  # Pause for 1 second
    
    
    # 3. Use press_sequentially to simulate a user typing
    print(f"--- Typing '{search_term}' into search bar... ---")
    await search_bar.press_sequentially(search_term, delay=random.randint(200, 400))

    # 4. Press Enter to submit the search
    await search_bar.press("Enter")

    # --- START OF FIX ---
    # ADD a robust wait here. 'networkidle' waits for the page to finish loading dynamic content.
    print("--- Search submitted. Waiting for results page to fully load... ---")
    await page.wait_for_load_state("domcontentloaded", timeout=30000)
    # --- END OF FIX ---

    # 3. Wait for search results
    print("--- Waiting for search results... ---")
    first_result = page.locator(".css-xrzmfa").first
    await first_result.wait_for(state="visible", timeout=15000)

    # --- START OF MODIFICATION ---
    # Prepare to capture the new page that opens after the click
    print("--- Expecting a new page to open... ---")
    async with page.context.expect_page() as new_page_info:
        await first_result.click()  # This action triggers the new page

    # Get the new page object from the event
    new_page = await new_page_info.value
    print(f"--- Switched focus to new page: {new_page.url} ---")

    # All subsequent actions must use this 'new_page' object
    await new_page.wait_for_load_state("domcontentloaded", timeout=20000)
    # --- END OF MODIFICATION ---

    print(f"--- Landed on product page for '{search_term}'. Starting pincode checks. ---")

    # After the page has loaded, check for and close any pop-ups.
    await check_and_close_intermittent_popup(new_page)
    # 5. Loop through the pincodes for this product and scrape the data
    group_results = []

    # Check if the product is unavailable before checking pincodes.
    config = SITE_CONFIG.get(site, {})
    unavailable_selector = config.get("unavailable_selector")
    if unavailable_selector:
        try:
            # Use a short timeout to quickly check for the "Notify Me" button.
            await new_page.locator(unavailable_selector).first.wait_for(state="visible", timeout=1000)
            print(f"--- Product is unavailable. Skipping all pincodes for this URL. ---")
            # Mark all pincodes for this URL as unavailable and return immediately.
            for _, row in pincode_group.iterrows():
                group_results.append({
                    "style_name": row["style_name"], "site_name": site,
                    "product_url": new_page.url, "pincode": row["pincode"],
                    "delivery_info": "Product Unavailable", "secondary_delivery_info": ""
                })
            return group_results # Skip to the next product
        except Exception:
            # If the selector is not found, the product is available. Proceed normally.
            pass

    consecutive_pincode_failures = 0
    #site = "Nykaa"

    for _, row in pincode_group.iterrows():
        if consecutive_pincode_failures >= 5:
            print(f"--- Aborting remaining pincodes for {search_term} due to failures. ---")
            break

        pincode = str(row["pincode"])
        # MODIFIED: Pass the 'new_page' object to your scraping function
        delivery_data = await scrape_pincode_on_page_nykaa(new_page, site, pincode)

        result = {
        #    "master_category": row["master_category"], "article_type": row["article_type"],
            "style_name": row["style_name"], "site_name": site,
            "product_url": new_page.url, "pincode": pincode,
            "delivery_info": delivery_data.get("primary", ""),
            "secondary_delivery_info": delivery_data.get("secondary", "")
        }
        group_results.append(result)

        if "Error" in result["delivery_info"] or "Not found" in result["delivery_info"]:
            consecutive_pincode_failures += 1
        else:
            consecutive_pincode_failures = 0

    return group_results
#Amazon Specific Functions
async def scrape_pincode_on_page_amz(page, site, pincode):
    """
    Attempts to enter a pincode using a deliberate, multi-step process to handle dynamic elements.
    """
    config = SITE_CONFIG.get(site)
    if not config:
        return {"primary": "Site not configured", "secondary": ""}

    # MODIFIED: Loop for up to 3 attempts (1 initial + 2 retries)
    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            # On retries (attempt > 0), the page is already reloaded by the except block.
            if attempt > 0:
                print(f"--- Starting Retry Attempt {attempt + 1}/{max_attempts} for pincode {pincode} ---")

            pre_click_selector = config.get("pre_pincode_click_selector")
            if pre_click_selector:
                try:
                    # Use a short timeout as this button may not be present on the first run.
                    await page.locator(pre_click_selector).first.click(timeout=10000)
                    #print("--- Clicked 'Change' button to enter a new pincode. ---")
                except Exception:
                    # This is expected if it's the first pincode check for this URL.
                    #print("--- No 'Change' button found, proceeding directly. ---")
                    pass
            
            pincode_input_element = page.locator(config["pincode_input_selector"]).first
            # Explicitly wait for the input box to be visible before interacting.
            print("--- Waiting for pincode input to be visible... ---")
            await pincode_input_element.wait_for(state="visible", timeout=7000)
            
            print("--- Pincode input is ready. Typing now. ---")
            await pincode_input_element.clear()
            #for char in pincode:
            #    await pincode_input_element.press(char, delay=random.randint(80, 250))
            await pincode_input_element.fill(pincode)
            await page.wait_for_timeout(500)

           #--- STEP 2: Click the submit/check button --- (earlier code)
            if config.get("pincode_submit_selector"):
               await page.locator(config["pincode_submit_selector"]).first.click()
            
            if site == "Amazon":
                try:
                    # First, wait for the pincode pop-up to disappear.
                    #print("--- Amazon: Waiting for pincode pop-up to close... ---")
                    await pincode_input_element.wait_for(state="hidden", timeout=5000)
                except Exception as e:
                    # If the pop-up doesn't close on its own, find and click the close button.
                    print("--- Amazon: Pop-up did not close. Clicking the manual close button. ---")
                    # This selector targets the <i class="a-icon a-icon-close"></i> element.
                    await page.locator("i.a-icon.a-icon-close").first.click()
                # Then, wait for the main page to finish reloading.
                print("--- Amazon: Waiting for main page to reload... ---")
                await page.wait_for_load_state('domcontentloaded', timeout=20000)
            
            #Wait for the page to stabilizes so information can be picked up
            await page.wait_for_timeout(1000)
            # NEW CHECK: After the page reloads for the new pincode, check if the product
            # has become 'Currently unavailable'. This is a high-priority status.

            unavailable_selector = config.get("unavailable_selector")
            if unavailable_selector:
                try:
                    # Use a very short timeout. We are checking if the element is *already* there,
                    # not waiting for it to appear.
                    unavailable_element = page.locator(unavailable_selector).first
                    await unavailable_element.wait_for(state="visible", timeout=500)
                    
                    # If the element is found, the product is unavailable for this pincode.
                    unavailable_text = (await unavailable_element.inner_text()).strip()
                    print(f"--- Pincode {pincode}: Product is now unavailable. ---")
                    return {"primary": unavailable_text, "secondary": ""} # Return immediately
                except Exception:
                    # This is the expected outcome if the product *is* available.
                    # We can safely continue to check for delivery dates.
                    pass
            # --- END OF MODIFICATION ---
            
      # --- Data Extraction ---
            # Initialize a dictionary to hold the results.
            results = {"primary": "Not found", "secondary": ""}
            
            # Get the prioritized list of selectors from your config.
            delivery_selectors = config.get("delivery_info_selectors", [])
            
            # Loop through each selector in the order they are listed.
            for item in delivery_selectors:
                item_type = item["type"]
                selector = item["selector"]
                
                try:
                    # Use a short timeout to quickly check if the element is visible.
                    element = page.locator(selector).first
                    await element.wait_for(state="visible", timeout=500) #changed the timeout here to make the script faster
                    
                    # If the element is found, grab its text.
                    text_content = (await element.inner_text()).strip()
                    
                    if item_type == "primary_delivery":
                        results["primary"] = text_content
                        
                    elif item_type == "unserviceable":
                        results["primary"] = text_content
                        # This is a final status. We can stop checking.
                        break
                        
                    elif item_type == "secondary_info":
                        results["secondary"] = text_content
                
                except Exception:
                    # It's normal for a selector not to be found. Just move to the next one.
                    pass

            # If we reach here, the attempt was successful.
            return results
        
        except Exception as e:
            # MODIFIED: Handle failure and decide whether to retry or fail permanently.
            print(f"--- Attempt {attempt + 1} for pincode {pincode} failed: {type(e).__name__} ---")
            if attempt < max_attempts - 1: # If this wasn't the last attempt
                print(f"--- Refreshing page and preparing for retry... ---")
                await page.reload(wait_until="domcontentloaded", timeout=60000)
            else: # This was the final attempt
                print(f"--- Final attempt for pincode {pincode} also failed.")
                return {"primary": f"Error: Failed after {max_attempts} attempts", "secondary": ""}
    
    # This line is reached if the loop finishes without a successful return.
    return {"primary": "Error: All retry attempts failed.", "secondary": ""}

#Function to return the best match for searched term
def find_best_match_index(search_term: str, results: list[str]) -> int:
    """
    Finds the index of the best partial match from a list of strings.
    The "best" is defined as the one with the most matching words.
    """
    search_words = set(search_term.lower().split())
    best_score = -1
    best_index = -1

    for i, title in enumerate(results):
        title_words = set(title.lower().split())
        # Calculate score as the number of common words
        score = len(search_words.intersection(title_words))

        if score > best_score:
            best_score = score
            best_index = i
            
    print(f"--- Best match scored {best_score} at index {best_index}: '{results[best_index] if best_index > -1 else 'None'}'")
    return best_index

async def search_and_scrape_amz(page, site, search_term, pincode_group):   
    """
    Searches for a product on Amazon.in, clicks the first result, 
    and then scrapes delivery speeds for a group of pincodes.
    """
    print(f"--- Searching for '{search_term}' on Amazon... ---")

    # 1. Navigate to the Amazon homepage
    await page.goto("https://www.amazon.in/M-A-C-Matte-Lipstick-Russian-Red/dp/B0006LNKYG/", wait_until="domcontentloaded", timeout=60000)

    # 2. Find and click the search bar to ensure it's focused
    # Locator updated for Amazon's search bar ID '#twotabsearchtextbox'
    search_bar = page.locator('#twotabsearchtextbox')
    await search_bar.click()

    # Add a deliberate pause to allow the page's scripts to initialize
    print("--- Pausing after click to let the search bar initialize... ---")
    await page.wait_for_timeout(1000)  # Pause for 1 second
    
    # 3. Use press_sequentially to simulate user typing
    print(f"--- Typing '{search_term}' into search bar... ---")
    await search_bar.press_sequentially(search_term, delay=random.randint(200, 400))

    # 4. Press Enter to submit the search
    await search_bar.press("Enter")

    # Wait for the search results page to finish loading dynamic content
    print("--- Search submitted. Waiting for results page to fully load... ---")
    await page.wait_for_load_state("domcontentloaded", timeout=60000)

    # 5. CRUCIAL FIX: Wait for the page navigation to a search URL to complete.
    # This confirms the search was successful and we are not stuck on the homepage or a CAPTCHA.
    # Amazon search URLs contain '/s?k='
    print("--- Waiting for the search results page to load (URL change)... ---")
    await page.wait_for_url("**/s?k=**", timeout=20000)
    
    # 6. Locate the product links using the new, highly specific locator.
    print("--- Locating all search results... ---")
    try:
        # THE FINAL, CORRECT LOCATOR BASED ON YOUR HTML
        results_locator = page.locator("div.s-result-item[data-asin] div[data-cy='title-recipe'] > a")
        
        # Wait for the first product link to become visible
        await results_locator.first.wait_for(state="visible", timeout=20000)
        print("--- First product link found. Proceeding to find best match...")

        # Get the text from all the visible results
        all_titles = await results_locator.all_inner_texts()
        
        if not all_titles:
            raise Exception("Located product links, but could not extract any titles.")

        print(f"--- Found {len(all_titles)} potential products. Evaluating...")
        best_match_index = find_best_match_index(search_term, all_titles)

        if best_match_index == -1:
            raise Exception(f"Could not find a suitable match for '{search_term}'.")
        
        best_match_locator = results_locator.nth(best_match_index)

        print(f"--- Clicking on the best match: '{all_titles[best_match_index]}' ---")
        async with page.context.expect_page() as new_page_info:
            await best_match_locator.scroll_into_view_if_needed() # Good practice before clicking
            await best_match_locator.click()

    except Exception as e:
        print(f"!!! CRITICAL FAILURE: Could not find or click a matching product. Error: {e}")
        await page.screenshot(path=f"error_screenshot_{search_term.replace(' ', '_')}.png")
        print("--- Screenshot saved for debugging. ---")
        raise e

    new_page = await new_page_info.value
    print(f"--- Switched focus to new page: {new_page.url} ---")
    
    # Wait for the product page to load
    await new_page.wait_for_load_state("domcontentloaded", timeout=20000)
    
    print(f"--- Landed on product page for '{search_term}'. Starting pincode checks. ---")

    # 5. Loop through the pincodes for this product and scrape the data
    group_results = []

    # Check if the product is unavailable before checking pincodes.
    config = SITE_CONFIG.get(site, {})
    unavailable_selector = config.get("unavailable_selector")
    if unavailable_selector:
        try:
            # Use a short timeout to quickly check for the "Notify Me" button.
            await new_page.locator(unavailable_selector).first.wait_for(state="visible", timeout=1000)
            print(f"--- Product is unavailable. Skipping all pincodes for this URL. ---")
            # Mark all pincodes for this URL as unavailable and return immediately.
            for _, row in pincode_group.iterrows():
                group_results.append({
                    "style_name": row["style_name"], "site_name": site,
                    "product_url": new_page.url, "pincode": row["pincode"],
                    "delivery_info": "Product Unavailable", "secondary_delivery_info": ""
                })
            return group_results # Skip to the next product
        except Exception:
            # If the selector is not found, the product is available. Proceed normally.
            pass

    consecutive_pincode_failures = 0
    #site = "Nykaa"

    for _, row in pincode_group.iterrows():
        if consecutive_pincode_failures >= 5:
            print(f"--- Aborting remaining pincodes for {search_term} due to failures. ---")
            break

        pincode = str(row["pincode"])
        # MODIFIED: Pass the 'new_page' object to your scraping function
        delivery_data = await scrape_pincode_on_page_amz(new_page, site, pincode)

        result = {
        #    "master_category": row["master_category"], "article_type": row["article_type"],
            "style_name": row["style_name"], "site_name": site,
            "product_url": new_page.url, "pincode": pincode,
            "delivery_info": delivery_data.get("primary", ""),
            "secondary_delivery_info": delivery_data.get("secondary", "")
        }
        group_results.append(result)

        if "Error" in result["delivery_info"] or "Not found" in result["delivery_info"]:
            consecutive_pincode_failures += 1
        else:
            consecutive_pincode_failures = 0

    return group_results

#Myntra Specific functions
# In Scraper_del_check.py, replace the entire scrape_pincode_on_page_myntra function

async def scrape_pincode_on_page_myntra(page, site, pincode):
    """
    Handles Myntra's pincode logic by waiting for the input box to be visible,
    clicking it to activate, and then filling it.
    """
    config = SITE_CONFIG.get(site)
    if not config:
        return {"primary": "Site not configured", "secondary": ""}

    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            if attempt > 0:
                print(f"--- Starting Retry Attempt {attempt + 1}/{max_attempts} for pincode {pincode} ---")

            # Give the page a moment for all dynamic content to render
           # await page.wait_for_timeout(random.uniform(900, 1000))    

            pre_click_selector = config.get("pre_pincode_click_selector")
            if pre_click_selector:
                try:
                    # Use a short timeout as this button may not be present on the first run.
                    await page.locator(pre_click_selector).first.click(timeout=3000)
                    #print("--- Clicked 'Change' button to enter a new pincode. ---")
                except Exception:
                    # This is expected if it's the first pincode check for this URL.
                    #print("--- No 'Change' button found, proceeding directly. ---")
                    pass    


            # --- START: THE CORRECTED LOGIC ---
            # 1. Directly locate the pincode input element.
            pincode_input_element = page.locator(config["pincode_input_selector"]).first
            
            # 2. Wait for this input box to become visible on the page.
            print("--- Waiting for the pincode input box to be visible... ---")
            await pincode_input_element.wait_for(state="visible", timeout=15000)

            # 3. Click the input box to ensure it is active and has focus.
            print("--- Clicking the pincode input box to activate... ---")
            await pincode_input_element.click()
            await page.wait_for_timeout(random.uniform(400, 700)) # Brief pause after clicking

            # 4. Now, fill the active input box with the pincode.
            print(f"--- Filling pincode {pincode}... ---")
            await pincode_input_element.fill(pincode)
            # --- END: THE CORRECTED LOGIC ---

            # 5. Click the submit button.
            if config.get("pincode_submit_selector"):
                await page.locator(config["pincode_submit_selector"]).first.click()

            #await page.wait_for_timeout(1000)

            # --- Data Extraction ---

            # --- START: MODIFIED DATA EXTRACTION ---
            # 1. Create a combined selector for both possible outcomes.
            unserviceable_selector = config["delivery_info_selectors"][0]["selector"]
            success_selector = config["delivery_info_selectors"][1]["selector"]
            combined_selector = f"{unserviceable_selector}, {success_selector}"

            # 2. Wait for EITHER the unserviceable message OR the success message to be visible.
            print("--- Waiting for delivery info to appear... ---")
            result_element = page.locator(combined_selector).first
            await result_element.wait_for(state="visible", timeout=10000)

            # 3. Now that a result is visible, run the original loop to capture the correct text.
            #    This loop is already designed to check for both types.
            print("--- Result is visible. Capturing text... ---")
            results = {"primary": "Not found", "secondary": ""}
            for item in config.get("delivery_info_selectors", []):
                try:
                    element = page.locator(item["selector"]).first
                    # Use a very short timeout here since we know one of the elements is already visible.
                    text_content = (await element.inner_text(timeout=1000)).strip()
                    
                    if item["type"] == "primary_delivery":
                        results["primary"] = text_content
                        break # Stop once we have a valid delivery date
                    elif item["type"] == "unserviceable":
                        results["primary"] = text_content
                        break # Stop once we know it's unserviceable
                except Exception:
                    pass
            
            print(f"--- Successfully captured: '{results['primary']}' ---")
            return results
            # --- END: MODIFIED DATA EXTRACTION ---

        except Exception as e:
            print(f"--- Attempt {attempt + 1} for pincode {pincode} failed: {type(e).__name__} ---")
            if attempt < max_attempts - 1:
                await page.reload(wait_until="domcontentloaded", timeout=60000)
            else:
                return {"primary": f"Error: Failed after {max_attempts} attempts", "secondary": ""}

    return {"primary": "Error: All retry attempts failed.", "secondary": ""}

async def search_and_scrape_myntra(page, site, search_term, pincode_group):
    """
    Searches for a product on Myntra, correctly captures the new product page,
    and then scrapes delivery speeds.
    """
    print(f"--- Searching for '{search_term}' on Myntra... ---")

    # 1. Navigate and search
    await page.goto("https://www.myntra.com/shirts/powerlook/powerlook-geometric-printed-short-sleeves-shirt/35802894/buy", wait_until="domcontentloaded", timeout=60000)
    search_bar = page.locator('input[placeholder="Search for products, brands and more"]')
    await search_bar.click()
    await page.wait_for_timeout(1000)
    await search_bar.press_sequentially(search_term, delay=random.randint(200, 400))
    await search_bar.press("Enter")
    await page.wait_for_load_state("domcontentloaded", timeout=30000)

    # 2. Wait for search results
    print("--- Waiting for search results... ---")
    # Using a more robust selector for the clickable product link
    first_result = page.locator("ul.results-base > li > a").first
    await first_result.wait_for(state="visible", timeout=15000)

    # --- START: THE CRITICAL FIX ---
    # 3. Expect a new page to open and capture its handle
    print("--- Expecting a new page to open... ---")
    async with page.context.expect_page() as new_page_info:
        await first_result.click()  # This action triggers the new page

    # Get the new page object from the event
    new_page = await new_page_info.value
    print(f"--- Switched focus to new page: {new_page.url} ---")

    # 4. All subsequent actions must use this 'new_page' object
    await new_page.wait_for_load_state("domcontentloaded", timeout=20000)
    # --- END: THE CRITICAL FIX ---

    print(f"--- Landed on product page. Starting pincode checks... ---")

    # 5. Loop through pincodes, passing the CORRECT page object
    group_results = []
    for _, row in pincode_group.iterrows():
        pincode = str(row["pincode"])
        # Pass the 'new_page' object to your scraping function
        delivery_data = await scrape_pincode_on_page_myntra(new_page, site, pincode)

        result = {
            "style_name": row["style_name"], "site_name": site,
            "product_url": new_page.url, "pincode": pincode,
            "delivery_info": delivery_data.get("primary", ""),
            "secondary_delivery_info": delivery_data.get("secondary", "")
        }
        group_results.append(result)

    return group_results

SCRAPER_WORKFLOWS = {
    "Nykaa": search_and_scrape_nykaa,
    "Amazon": search_and_scrape_amz,
    "Myntra": search_and_scrape_myntra
    # Add other site workflows here, e.g., "Flipkart": search_and_scrape_flipkart
}
# ==============================================================================
#  THE REFACTORED SCRAPER FUNCTION
# ==============================================================================

# In Scraper_del_check.py, add this new helper function right BEFORE your main_scraper_func

async def run_scrape_task(browser, site, search_term, group, pass_num):
    """
    A wrapper for a single scraping task. It creates an isolated browser context,
    runs the designated scraper workflow, and handles errors for one task without
    stopping others.
    """
    context = await browser.new_context(user_agent=random.choice(USER_AGENTS))
    page = await context.new_page()
    try:
        print(f"\n[Pass {pass_num}] Starting task for product: '{search_term}' on site: '{site}'...")
        scraper_function = SCRAPER_WORKFLOWS.get(site)
        if scraper_function:
            # Call the appropriate search function (e.g., search_and_scrape_myntra)
            return await scraper_function(page, site, search_term, group)
        else:
            # Handle cases where a site is not configured
            print(f"!!! No scraper workflow defined for site: '{site}'. Skipping. !!!")
            results = []
            for _, row in group.iterrows():
                results.append({
                    "style_name": row["style_name"], "site_name": site,
                    "product_url": "N/A", "pincode": row["pincode"],
                    "delivery_info": f"Error: No scraper configured", "secondary_delivery_info": ""
                })
            return results
    except Exception as e:
        print(f"!!! CRITICAL FAILURE in task for '{search_term}' on '{site}'. Error: {e}")
        # Return an error result for each pincode in the failed group
        error_results = []
        for _, row in group.iterrows():
            error_results.append({
                "style_name": row["style_name"], "site_name": site,
                "product_url": "N/A - Task Failed", "pincode": row["pincode"],
                "delivery_info": f"Error: Task failed critically", "secondary_delivery_info": ""
            })
        return error_results
    finally:
        await context.close()


# NOW, REPLACE your existing main_scraper_func with this new parallel version
async def main_scraper_func(input_df: pd.DataFrame) -> pd.DataFrame:
    """
    Main scraper function that runs all scraping tasks for each product-site
    combination in parallel to significantly speed up the process.
    """
    start_time = time.monotonic()
    all_results_list = []

    for pass_num in [1, 2]:
        tasks_df = pd.DataFrame()
        if pass_num == 1:
            tasks_df = input_df
            print("\n" + "="*20 + f" STARTING PASS 1 WITH {len(tasks_df.groupby(['site_name', 'style_name']))} PARALLEL TASKS " + "="*20)
        else:
            if not all_results_list: break
            pass_1_df = pd.DataFrame(all_results_list)
            tasks_df = pass_1_df[pass_1_df['delivery_info'].str.startswith("Error:", na=False)].copy()
            if tasks_df.empty:
                print("\nNo failed tasks to retry.")
                break
            print("\n" + "="*20 + f" STARTING PASS 2: RETRYING {len(tasks_df.groupby(['site_name', 'style_name']))} FAILED TASKS IN PARALLEL " + "="*20)

        async with async_playwright() as p:
           # browser = await p.chromium.launch(headless=False) 
           #launch wihout being visible
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    "--window-position=2000,2000",
                    "--window-size=100,100"
                ]
            )
            
            # 1. Prepare all tasks without running them yet
            tasks_to_run = []
            for (site, search_term), group in tasks_df.groupby(['site_name', 'style_name']):
                task = run_scrape_task(browser, site, search_term, group, pass_num)
                tasks_to_run.append(task)
            
            # 2. Run all prepared tasks concurrently
            if tasks_to_run:
                results_from_tasks = await asyncio.gather(*tasks_to_run)
                # Flatten the list of lists into a single list
                all_results_list.extend([item for sublist in results_from_tasks for item in sublist])
            
            await browser.close()

    if not all_results_list:
        return pd.DataFrame()

    # Post-processing remains the same
    final_results_df = pd.DataFrame(all_results_list).drop_duplicates(subset=['site_name', 'style_name', 'pincode'], keep='last')
    final_results_df['scrape_date'] = pd.to_datetime(date.today())
    final_results_df['delivery_date'] = final_results_df.apply(extract_delivery_date, axis=1)
    final_results_df['days_to_delivery'] = (pd.to_datetime(final_results_df['delivery_date'], errors='coerce') - final_results_df['scrape_date']).dt.days

    duration = time.monotonic() - start_time
    minutes, seconds = divmod(duration, 60)
    print("\n" + "="*50)
    print(f"Total parallel scraper execution time: {int(minutes)} minutes and {int(seconds)} seconds.")
    print("="*50)

    return final_results_df, duration 

# ==============================================================================
#  OPTIONAL: TEST BLOCK TO RUN THIS SCRIPT STANDALONE
# ==============================================================================

# # Cell 2: Create input and run the scraper
# import pandas as pd

# # Create a dummy input DataFrame, just like Streamlit would
# test_data = {
#     'master_category': ['Apparel', 'Apparel'],
#     'article_type': ['Jeans', 'Jeans'],
#     'style_name': ['H&M Women Straight High Jeans', 'H&M Women Straight High Jeans'],
#     'site_name': ['Nykaa', 'Nykaa'],
#     'pincode': ['201301', '700020']
# }
# test_df = pd.DataFrame(test_data)

# print("Starting the scrape from the notebook...")

# # Use 'await' directly on the function call
# # DO NOT use asyncio.run() here
# results_df = asyncio.run(main_scraper_func(test_df))

# print("Scraping complete!")
# print("\n--- SCRAPER FUNCTION RETURNED THE FOLLOWING DATAFRAME ---")
# print(results_df)


# if __name__ == '__main__':
#     # This block allows you to test your scraper without running the Streamlit app.
#     # It will only run when you execute `python scraper.py` directly.

#     # 1. Create a dummy input DataFrame, just like Streamlit would
#     test_data = {
#         'master_category': ['Apparel'],
#         'article_type': ['Jeans'],
#         'style_name': ['H&M Women Straight High Jeans'],
#         'site_name': ['Nykaa'],
#         'pincode': ['201301']
#     }
#     test_df = pd.DataFrame(test_data)

#     print("--- RUNNING SCRAPER IN STANDALONE TEST MODE ---")

#     # 2. Call the scraper function with the test data
#     results_df = await main_scraper_func(test_df)

#     # 3. Print the results to the console
#     print("Scraping complete!")
#     print("\n--- SCRAPER FUNCTION RETURNED THE FOLLOWING DATAFRAME ---")
#     print(results)


# In[ ]:




