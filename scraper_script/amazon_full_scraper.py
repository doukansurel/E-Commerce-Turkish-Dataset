import concurrent.futures
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import csv
import re
from datetime import datetime
import random

# ═══════════════════════════════════════════════════════════════════════════════
# AYARLAR (M3 PRO 8GB İÇİN OPTİMİZE EDİLDİ)
# ═══════════════════════════════════════════════════════════════════════════════

# 8GB RAM olduğu için aynı anda en fazla 3 tarayıcı açıyoruz.
MAX_WORKERS = 3 
TARGET_CATEGORIES = ["Moda"]
SCRAPE_REVIEWS = True 
# Test için None yerine sayı yazabilirsin (örn: 20). None = Hepsi
MAX_PRODUCTS_FOR_REVIEWS = None 

# ═══════════════════════════════════════════════════════════════════════════════
# DRIVER KURULUMU
# ═══════════════════════════════════════════════════════════════════════════════

def create_optimized_driver(headless=False):
    """Creates an optimized Chrome driver with anti-detection measures."""
    service = Service(ChromeDriverManager().install())
    options = webdriver.ChromeOptions()
    
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--lang=tr-TR')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-infobars')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    if headless:
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
        # RAM ve Hız için görselleri yüklemeyi kapat (Sadece headless modda)
        options.add_argument('--blink-settings=imagesEnabled=false')
    
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(service=service, options=options)
    
    driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
        'source': '''
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        '''
    })
    
    return driver


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 1: LOGIN
# ═══════════════════════════════════════════════════════════════════════════════

def login_to_amazon(driver):
    """Opens Amazon and waits for user to login manually."""
    
    print("\n" + "="*60)
    print("🔐 STEP 1: LOGIN TO AMAZON")
    print("="*60)
    
    driver.get("https://www.amazon.com.tr/")
    time.sleep(2)
    
    try:
        login_btn = driver.find_element(By.CSS_SELECTOR, "#nav-link-accountList, a[data-nav-role='signin']")
        login_btn.click()
        time.sleep(2)
    except:
        pass
    
    print("\n" + "-"*60)
    print("⚠️  MANUAL ACTION REQUIRED:")
    print("   Please Login manually in the browser.")
    print("-"*60)
    
    input("\n✅ Press ENTER when you're logged in and ready to continue...")
    print("\n🎉 Login complete! Continuing...")
    time.sleep(2)


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 2 & 3: NAVIGATION & URL COLLECTION HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def navigate_to_bestsellers(driver):
    try:
        bestsellers_btn = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "a[href*='bestsellers']"))
        )
        bestsellers_btn.click()
        time.sleep(3)
    except:
        driver.get("https://www.amazon.com.tr/gp/bestsellers/")
        time.sleep(3)

def get_main_categories_from_page(driver):
    categories = []
    try:
        category_links = driver.find_elements(By.CSS_SELECTOR, "[id^='CardInstance'] a[href*='/gp/bestsellers/']")
        for link in category_links:
            try:
                href = link.get_attribute('href')
                name = link.text.strip()
                if href and name:
                    categories.append({'name': name, 'url': href})
            except: continue
        
        # Remove duplicates
        seen = set()
        unique = []
        for cat in categories:
            if cat['url'] not in seen:
                seen.add(cat['url'])
                unique.append(cat)
        return unique
    except: return []

def get_subcategories(driver):
    subcategories = []
    try:
        time.sleep(1)
        subcat_links = driver.find_elements(By.CSS_SELECTOR, "ul[class*='zg-browse-group'] li[class*='zg-browse-item'] a[href*='/gp/bestsellers/']")
        for link in subcat_links:
            try:
                href = link.get_attribute('href')
                name = link.text.strip()
                if href and name:
                    subcategories.append({'name': name, 'url': href})
            except: continue
        
        seen = set()
        unique = []
        for sub in subcategories:
            if sub['url'] not in seen:
                seen.add(sub['url'])
                unique.append(sub)
        return unique
    except: return []

def scroll_to_bottom(driver):
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height: break
        last_height = new_height
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(0.5)

def get_product_urls_from_page(driver):
    urls = []
    product_cards = driver.find_elements(By.CSS_SELECTOR, "[data-asin]:not([data-asin=''])")
    for card in product_cards:
        try:
            asin = card.get_attribute('data-asin')
            try:
                href = card.find_element(By.CSS_SELECTOR, "a[href*='/dp/']").get_attribute('href')
                clean_url = href.split('/ref=')[0]
            except:
                clean_url = f"https://www.amazon.com.tr/dp/{asin}"
            
            try:
                name = card.find_element(By.CSS_SELECTOR, "[class*='p13n-sc-css-line-clamp']").text
            except:
                name = asin
            
            urls.append({'asin': asin, 'url': clean_url, 'name': name})
        except: continue
    return urls

def get_all_product_urls_with_pagination(driver, category_name, subcategory_name):
    all_urls = []
    current_page = 1
    while True:
        scroll_to_bottom(driver)
        page_urls = get_product_urls_from_page(driver)
        for url_data in page_urls:
            url_data['category'] = category_name
            url_data['subcategory'] = subcategory_name
            url_data['page'] = current_page
        all_urls.extend(page_urls)
        
        try:
            next_links = driver.find_elements(By.CSS_SELECTOR, "ul.a-pagination li.a-last a")
            if next_links and next_links[0].is_displayed():
                driver.execute_script("arguments[0].click();", next_links[0])
                time.sleep(2)
                current_page += 1
            else: break
        except: break
    return all_urls

def scrape_category_with_subcategories(driver, category_url, category_name):
    all_urls = []
    print(f"\n   📂 CATEGORY: {category_name}")
    driver.get(category_url)
    time.sleep(3)
    
    subcategories = get_subcategories(driver)
    if not subcategories: return all_urls
    
    for i, subcat in enumerate(subcategories, 1):
        try:
            print(f"      📁 [{i}/{len(subcategories)}] {subcat['name']}")
            driver.get(subcat['url'])
            time.sleep(2)
            subcat_urls = get_all_product_urls_with_pagination(driver, category_name, subcat['name'])
            all_urls.extend(subcat_urls)
            print(f"         📊 Subtotal: {len(subcat_urls)} products")
        except: continue
    return all_urls


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 4: PARALLEL REVIEW SCRAPING WORKER
# ═══════════════════════════════════════════════════════════════════════════════

def extract_asin_from_url(url):
    patterns = [r'/dp/([A-Z0-9]{10})', r'/product/([A-Z0-9]{10})', r'/gp/product/([A-Z0-9]{10})']
    for pattern in patterns:
        match = re.search(pattern, url)
        if match: return match.group(1)
    return None

def extract_rating(rating_text):
    try:
        match = re.search(r'(\d+[,.]?\d*)\s*$', rating_text)
        if match: return match.group(1).replace(',', '.')
    except: pass
    return rating_text

def extract_date(date_text):
    try:
        return date_text.replace("Türkiye'de", "").replace("tarihinde değerlendirildi", "").strip()
    except: return date_text

def get_reviews_from_page(driver):
    """(Helper used by worker) Extracts reviews from current page."""
    reviews = []
    review_cards = driver.find_elements(By.CSS_SELECTOR, "[data-hook='mobley-review-content'], [data-hook='review']")
    
    for card in review_cards:
        try:
            review = {}
            review['review_id'] = card.get_attribute('id')
            review['username'] = card.find_element(By.CSS_SELECTOR, ".a-profile-name").text
            
            try:
                rating_elem = card.find_element(By.CSS_SELECTOR, "[data-hook='review-star-rating'] .a-icon-alt, .review-rating .a-icon-alt")
                review['rating'] = extract_rating(rating_elem.get_attribute('innerHTML') or rating_elem.text)
            except: review['rating'] = ''
            
            try:
                review['date'] = extract_date(card.find_element(By.CSS_SELECTOR, "[data-hook='review-date']").text)
            except: review['date'] = ''
            
            try:
                review['body'] = card.find_element(By.CSS_SELECTOR, "[data-hook='review-body']").text.strip()
            except: review['body'] = ''
            
            if review['username'] or review['body']:
                reviews.append(review)
        except: continue
    return reviews

def scrape_single_product_worker(product_info, cookies):
    """
    THREAD WORKER: Scrapes reviews for one product using its own driver instance.
    """
    driver = None
    product_reviews = []
    
    try:
        # Create a new headless driver for this thread
        driver = create_optimized_driver(headless=True)
        
        # 1. Navigate to a dummy page on domain to set cookies
        driver.get("https://www.amazon.com.tr/404")
        
        # 2. Inject cookies from the main session
        for cookie in cookies:
            try:
                driver.add_cookie(cookie)
            except: pass
            
        # 3. Navigate to reviews page
        asin = product_info.get('asin') or extract_asin_from_url(product_info['url'])
        reviews_url = f"https://www.amazon.com.tr/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews"
        
        driver.get(reviews_url)
        # Random sleep to behave human-like inside thread
        time.sleep(random.uniform(1.5, 3.0))
        
        page = 1
        # RAM saving: limit pages per product (optional, remove check to scrape all)
        while page <= 5: 
            # Scroll logic inside thread
            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height: break
                last_height = new_height
            driver.execute_script("window.scrollTo(0, 0);")
            
            # Extract
            page_reviews = get_reviews_from_page(driver)
            
            for review in page_reviews:
                review['asin'] = asin
                review['product_name'] = product_info.get('name', '')
                review['product_url'] = product_info.get('url', '')
            
            product_reviews.extend(page_reviews)
            
            # Next Page
            try:
                next_btn = driver.find_element(By.CSS_SELECTOR, "li.a-last a")
                if next_btn.is_displayed():
                    driver.execute_script("arguments[0].click();", next_btn)
                    time.sleep(2)
                    page += 1
                else: break
            except: break
            
    except Exception as e:
        # Silently fail or log error for this specific product
        # print(f"Error in thread for {product_info.get('asin')}: {e}")
        pass
        
    finally:
        if driver:
            driver.quit()
            
    return product_reviews


# ═══════════════════════════════════════════════════════════════════════════════
# STEP 5: SAVE DATA
# ═══════════════════════════════════════════════════════════════════════════════

def save_urls_to_csv(all_urls, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['category', 'subcategory', 'page', 'asin', 'name', 'url'])
        writer.writeheader()
        writer.writerows(all_urls)
    print(f"💾 Saved {len(all_urls)} URLs to {filename}")

def save_reviews_to_csv(reviews, filename):
    if not reviews: return
    fieldnames = ['asin', 'product_name', 'review_id', 'username', 'rating', 'date', 'body', 'product_url']
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(reviews)
    print(f"💾 Saved {len(reviews)} reviews to {filename}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN FUNCTION
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("\n" + "="*70)
    print(f"🚀 AMAZON SCRAPER - PARALLEL MODE (Max Workers: {MAX_WORKERS})")
    print("="*70)
    
    driver = create_optimized_driver(headless=False)
    all_urls = []
    
    try:
        # ─────────────────────────────────────────────────────────────
        # STEP 1: LOGIN & GET COOKIES
        # ─────────────────────────────────────────────────────────────
        login_to_amazon(driver)
        
        # 💡 CRITICAL: Save cookies to transfer to threads later
        print("🍪 Capturing session cookies...")
        main_cookies = driver.get_cookies()
        
        # ─────────────────────────────────────────────────────────────
        # STEP 2 & 3: COLLECT PRODUCT URLS
        # ─────────────────────────────────────────────────────────────
        navigate_to_bestsellers(driver)
        all_main_categories = get_main_categories_from_page(driver)
        
        filtered_categories = [c for c in all_main_categories if c['name'] in TARGET_CATEGORIES]
        print(f"\n🎯 Target categories: {[c['name'] for c in filtered_categories]}")
        
        for category in filtered_categories:
            urls = scrape_category_with_subcategories(driver, category['url'], category['name'])
            all_urls.extend(urls)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        urls_filename = f"amazon_urls_{timestamp}.csv"
        save_urls_to_csv(all_urls, urls_filename)
        
    except Exception as e:
        print(f"\n❌ Error during URL collection: {e}")
        driver.quit()
        return

    # ─────────────────────────────────────────────────────────────
    # CRITICAL MEMORY OPTIMIZATION FOR 8GB RAM
    # ─────────────────────────────────────────────────────────────
    print("\n💤 Closing Main Browser to free up RAM for parallel workers...")
    driver.quit() 
    time.sleep(2) 
    # Now your RAM is free for the threads!

    # ─────────────────────────────────────────────────────────────
    # STEP 4: PARALLEL REVIEW SCRAPING
    # ─────────────────────────────────────────────────────────────
    if SCRAPE_REVIEWS and all_urls:
        print("\n" + "="*70)
        print(f"📝 STEP 4: PARALLEL REVIEW SCRAPING ({MAX_WORKERS} Threads)")
        print("="*70)
        
        all_reviews = []
        products_to_scrape = all_urls[:MAX_PRODUCTS_FOR_REVIEWS] if MAX_PRODUCTS_FOR_REVIEWS else all_urls
        total = len(products_to_scrape)
        completed = 0
        
        # Using ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_product = {
                executor.submit(scrape_single_product_worker, p, main_cookies): p 
                for p in products_to_scrape
            }
            
            # Process as they finish
            for future in concurrent.futures.as_completed(future_to_product):
                product = future_to_product[future]
                completed += 1
                try:
                    data = future.result()
                    all_reviews.extend(data)
                    print(f"   ✅ [{completed}/{total}] {product['name'][:30]}... ({len(data)} reviews)")
                except Exception as exc:
                    print(f"   ❌ [{completed}/{total}] Error: {exc}")

        # Save reviews
        reviews_filename = f"amazon_reviews_{timestamp}.csv"
        save_reviews_to_csv(all_reviews, reviews_filename)
        
        print(f"\n📊 Total reviews collected: {len(all_reviews)}")

    print("\n✅ DONE!")

if __name__ == "__main__":
    main()