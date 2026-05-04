"""
Trendyol Full Scraper
=====================
Bu script Trendyol'un en çok satanlar kategorisine gidip,
tüm kategorilerdeki en çok değerlendirilen ürünlerin yorumlarını toplar.

Kullanım:
    python trendyol_full_scraper.py
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.action_chains import ActionChains
from bs4 import BeautifulSoup
import time
import csv
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
import gc

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


@dataclass
class Product:
    """Ürün bilgilerini tutan veri sınıfı"""
    url: str
    category: str
    name: str = ""
    review_count: int = 0


@dataclass
class Review:
    """Yorum bilgilerini tutan veri sınıfı"""
    category: str
    product_url: str
    user: str
    date: str
    point: Optional[int]
    comment: str
    seller: str
    image_count: int
    seller_rate: str


class TrendyolDriver:
    """
    Chrome WebDriver yönetimi için sınıf.
    Anti-bot önlemleri ve optimizasyonlar içerir.
    """
    
    def __init__(self, headless: bool = False):
        """
        Args:
            headless: True ise tarayıcı arka planda çalışır (Trendyol engelleyebilir)
        """
        self.headless = headless
        self.driver = None
    
    def create(self) -> webdriver.Chrome:
        """Chrome driver oluşturur ve döndürür."""
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        
        # Anti-bot önlemleri
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--lang=tr-TR')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-infobars')
        options.add_argument('--window-size=1920,1080')
        
        # Gerçek tarayıcı user-agent
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        if self.headless:
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
        
        # Resimleri engelle (performans için)
        prefs = {
            'profile.managed_default_content_settings.images': 2,
        }
        options.add_experimental_option('prefs', prefs)
        options.add_experimental_option('excludeSwitches', ['enable-automation'])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(service=service, options=options)
        
        # Selenium tespitini gizle
        self.driver.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument', {
            'source': '''
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
            '''
        })
        
        logger.info("Chrome driver başarıyla oluşturuldu")
        return self.driver
    
    def close_popup(self):
        """Trendyol popup'ını kapatmak için tıklama yapar."""
        if self.driver:
            try:
                actions = ActionChains(self.driver)
                actions.move_by_offset(10, 10).click().perform()
                actions.reset_actions()
                time.sleep(2)
                logger.info("Popup kapatıldı")
            except Exception as e:
                logger.warning(f"Popup kapatılamadı: {e}")
    
    def is_alive(self) -> bool:
        """Driver'ın hala açık olup olmadığını kontrol eder."""
        if not self.driver:
            return False
        try:
            _ = self.driver.current_url
            return True
        except:
            return False
    
    def quit(self):
        """Driver'ı kapatır."""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Driver kapatıldı")
            except:
                pass
            self.driver = None


class TrendyolCategoryScraper:
    """
    Trendyol kategorilerini ve ürün listelerini çeken sınıf.
    """
    
    BASE_URL = "https://www.trendyol.com"
    
    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver
        self.categories: List[str] = []
        self.products: Dict[str, List[str]] = {}  # category -> [product_urls]
    
    def go_to_best_sellers(self) -> bool:
        """
        Ana sayfadan 'Çok Satanlar' bölümüne gider.
        
        Returns:
            bool: Başarılı ise True
        """
        try:
            self.driver.get(self.BASE_URL)
            logger.info(f"Ana sayfa açıldı: {self.BASE_URL}")
            time.sleep(3)
            
            # Popup'ı kapat
            self._close_popup()
            time.sleep(5)
            
            # Çok satanlar butonuna tıkla
            best_seller_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, 'a[href*="cok-satanlar"]'))
            )
            best_seller_btn.click()
            time.sleep(5)
            logger.info("'Çok Satanlar' sayfasına gidildi")
            return True
            
        except Exception as e:
            logger.error(f"Çok satanlar sayfasına gidilemedi: {e}")
            return False
    
    def get_categories(self) -> List[str]:
        """
        Mevcut kategorileri bulur.
        "Popüler Ürünler" kategorisini dahil etmez.
        
        Returns:
            List[str]: Kategori adları listesi
        """
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'button.category-pill'))
            )
            
            category_elements = self.driver.find_elements(By.CSS_SELECTOR, 'button.category-pill')
            
            # Popüler Ürünler kategorisini dahil etme
            self.categories = []
            for elem in category_elements:
                text = elem.text.strip()
                # "Popüler" içeren kategorileri atla
                if text and "Popüler" not in text and "popüler" not in text:
                    self.categories.append(text)
            
            logger.info(f"{len(self.categories)} kategori bulundu (Popüler Ürünler hariç): {self.categories}")
            return self.categories
            
        except Exception as e:
            logger.error(f"Kategoriler bulunamadı: {e}")
            return []
    
    def select_category_by_name(self, category_name: str) -> bool:
        """
        Belirtilen isimdeki kategoriyi seçer.
        
        Args:
            category_name: Kategori adı
            
        Returns:
            bool: Başarılı ise True
        """
        try:
            category_elements = self.driver.find_elements(By.CSS_SELECTOR, 'button.category-pill')
            
            for elem in category_elements:
                if elem.text.strip() == category_name:
                    elem.click()
                    time.sleep(3)
                    logger.info(f"Kategori seçildi: {category_name}")
                    return True
            
            logger.error(f"Kategori bulunamadı: {category_name}")
            return False
            
        except Exception as e:
            logger.error(f"Kategori seçilemedi: {e}")
            return False
    
    def click_most_reviewed(self) -> bool:
        """
        'En Çok Değerlendirilenler' butonuna tıklar.
        
        Returns:
            bool: Başarılı ise True
        """
        try:
            most_reviewed_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "En Çok Değerlendirilenler")]'))
            )
            most_reviewed_btn.click()
            time.sleep(3)
            logger.info("'En Çok Değerlendirilenler' seçildi")
            return True
            
        except Exception as e:
            logger.error(f"'En Çok Değerlendirilenler' bulunamadı: {e}")
            return False
    
    def get_product_urls(self, max_products: Optional[int] = None) -> List[str]:
        """
        Sayfadaki TÜM ürün URL'lerini toplar (sayfa sonuna kadar scroll yaparak).
        
        Args:
            max_products: Maksimum toplanacak ürün sayısı (None = tümü)
            
        Returns:
            List[str]: Ürün URL'leri
        """
        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'div.top-ranking-product-list>a'))
            )
            
            all_hrefs: Set[str] = set()
            scroll_pause = 1.5
            scroll_step = 500
            no_new_count = 0  # Yeni ürün gelmezse sayacı
            
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            
            while True:
                # Ürünleri topla
                products = self.driver.find_elements(By.CSS_SELECTOR, 'div.top-ranking-product-list a')
                old_count = len(all_hrefs)
                
                for product in products:
                    href = product.get_attribute('href')
                    if href:
                        all_hrefs.add(href)
                
                new_count = len(all_hrefs)
                logger.info(f"Scroll pozisyonu: {current_position}, Bulunan ürün: {new_count}")
                
                # max_products kontrolü
                if max_products and new_count >= max_products:
                    logger.info(f"Maksimum ürün sayısına ulaşıldı: {max_products}")
                    break
                
                # Scroll yap
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(scroll_pause)
                
                # Sayfa yüksekliğini kontrol et
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                
                # Yeni ürün gelmediyse say
                if new_count == old_count:
                    no_new_count += 1
                else:
                    no_new_count = 0
                
                # Sayfa sonu kontrolü: yükseklik değişmedi VE 3 scroll boyunca yeni ürün gelmedi
                if current_position >= new_height and no_new_count >= 3:
                    logger.info("Sayfa sonuna ulaşıldı")
                    break
                
                # Güvenlik: Çok fazla scroll yapma (max 100 ürün civarı için)
                if current_position > 50000:
                    logger.warning("Maksimum scroll limitine ulaşıldı")
                    break
                
                last_height = new_height
            
            # Son kez topla
            products = self.driver.find_elements(By.CSS_SELECTOR, 'div.top-ranking-product-list a')
            for product in products:
                href = product.get_attribute('href')
                if href:
                    all_hrefs.add(href)
            
            urls = list(all_hrefs)
            if max_products:
                urls = urls[:max_products]
            
            logger.info(f"TOPLAM {len(urls)} ürün URL'si toplandı")
            return urls
            
        except Exception as e:
            logger.error(f"Ürün URL'leri toplanamadı: {e}")
            return []
    
    def get_all_category_products(
        self, 
        category_names: Optional[List[str]] = None,
        max_products_per_category: Optional[int] = None
    ) -> List[Product]:
        """
        Belirtilen kategorilerdeki tüm ürünleri toplar.
        "Popüler Ürünler" kategorisi otomatik olarak hariç tutulur.
        
        Args:
            category_names: Toplanacak kategori isimleri (None ise tüm kategoriler)
            max_products_per_category: Her kategoriden maksimum ürün sayısı (None = tümü)
            
        Returns:
            List[Product]: Ürün listesi
        """
        all_products: List[Product] = []
        
        if not self.go_to_best_sellers():
            return all_products
        
        # Kategorileri al (Popüler Ürünler otomatik hariç)
        available_categories = self.get_categories()
        
        if not available_categories:
            return all_products
        
        # Hangi kategorileri işleyeceğimizi belirle
        if category_names is None:
            categories_to_process = available_categories
        else:
            categories_to_process = [c for c in category_names if c in available_categories]
        
        total_categories = len(categories_to_process)
        
        for idx, category_name in enumerate(categories_to_process, 1):
            logger.info(f"\n{'='*50}")
            logger.info(f"Kategori işleniyor: {category_name} ({idx}/{total_categories})")
            
            # Kategoriye git (isimle)
            if not self.select_category_by_name(category_name):
                # Tekrar ana sayfaya dön ve devam et
                if idx < total_categories:
                    self.go_to_best_sellers()
                continue
            
            # En çok değerlendirilenler
            if not self.click_most_reviewed():
                if idx < total_categories:
                    self.go_to_best_sellers()
                continue
            
            # Ürün URL'lerini topla (scroll ile TÜMÜ)
            urls = self.get_product_urls(max_products_per_category)
            
            for url in urls:
                product = Product(url=url, category=category_name)
                all_products.append(product)
            
            logger.info(f"'{category_name}' kategorisinden {len(urls)} ürün eklendi")
            
            # Tekrar ana sayfaya dön (son kategori değilse)
            if idx < total_categories:
                self.go_to_best_sellers()
        
        logger.info(f"\n{'='*50}")
        logger.info(f"TOPLAM {len(all_products)} ürün toplandı")
        return all_products
    
    def _close_popup(self):
        """Popup kapatma helper metodu"""
        try:
            actions = ActionChains(self.driver)
            actions.move_by_offset(10, 10).click().perform()
            actions.reset_actions()
            time.sleep(2)
        except:
            pass


class TrendyolReviewScraper:
    """
    Trendyol ürün yorumlarını çeken sınıf.
    """
    
    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver
        self.processed_comments: Set[str] = set()
    
    def navigate_to_reviews(self, product_url: str) -> bool:
        """
        Ürün sayfasına gidip yorum bölümünü açar.
        
        Args:
            product_url: Ürün URL'si
            
        Returns:
            bool: Başarılı ise True
        """
        try:
            # URL'de /yorumlar varsa direkt git
            if '/yorumlar' in product_url:
                reviews_url = product_url
            else:
                # URL'ye /yorumlar ekle
                # Örnek: https://www.trendyol.com/brand/product-p-123456 -> 
                #        https://www.trendyol.com/brand/product-p-123456/yorumlar
                if '?' in product_url:
                    base_url, query = product_url.split('?', 1)
                    reviews_url = f"{base_url}/yorumlar?{query}"
                else:
                    reviews_url = f"{product_url}/yorumlar"
            
            logger.info(f"Yorum URL'si: {reviews_url[:80]}...")
            self.driver.get(reviews_url)
            time.sleep(3)
            
            # Popup kapat
            self._close_popup()
            time.sleep(3)
            
            logger.info("Yorum sayfası açıldı")
            return True
            
        except Exception as e:
            logger.error(f"Yorum sayfasına gidilemedi: {e}")
            return False
    
    def get_seller_rate(self) -> str:
        """Satıcı puanını alır."""
        try:
            rate_elem = self.driver.find_element(By.CSS_SELECTOR, '.rate-value')
            return rate_elem.text
        except:
            return "N/A"
    
    def apply_seller_filter(self) -> bool:
        """
        'Sadece bu satıcının yorumları' filtresini uygular.
        
        Returns:
            bool: Başarılı ise True
        """
        checkbox_selectors = [
            '[data-testid="checkbox"]',
            '.merchant-checkbox input',
            '.seller-filter input[type="checkbox"]',
            '.rnr-com-seller-checkbox input',
            'input[type="checkbox"]',
            '.checkbox-container input'
        ]
        
        for selector in checkbox_selectors:
            try:
                checkbox = WebDriverWait(self.driver, 2).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                )
                checkbox.click()
                time.sleep(2)
                logger.info("Satıcı filtresi uygulandı")
                return True
            except:
                continue
        
        # Label ile dene
        try:
            label = self.driver.find_element(
                By.XPATH, 
                "//label[contains(text(), 'satıcı') or contains(text(), 'Satıcı')]"
            )
            label.click()
            time.sleep(2)
            logger.info("Satıcı filtresi (label) uygulandı")
            return True
        except:
            pass
        
        logger.warning("Satıcı filtresi uygulanamadı")
        return False
    
    def scrape_reviews(
        self, 
        product: Product,
        csv_writer,
        csv_file,
        apply_seller_filter: bool = True
    ) -> int:
        """
        Bir üründen tüm yorumları çeker ve CSV'ye yazar.
        
        Args:
            product: Ürün bilgileri
            csv_writer: CSV writer objesi
            csv_file: CSV dosya objesi
            apply_seller_filter: Satıcı filtresi uygulansın mı
            
        Returns:
            int: Çekilen yorum sayısı
        """
        review_count = 0
        self.processed_comments.clear()
        
        try:
            if not self.navigate_to_reviews(product.url):
                return 0
            
            # Popup kapat
            self._close_popup()
            time.sleep(3)
            
            # Yorumların yüklenmesini bekle
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.review, .pr-rnr-com'))
                )
            except:
                logger.warning("Yorum elementi bulunamadı")
            
            # Satıcı puanı
            seller_rate = self.get_seller_rate()
            logger.info(f"Satıcı puanı: {seller_rate}")
            
            # Satıcı filtresi
            if apply_seller_filter:
                self.apply_seller_filter()
            
            # Scroll ve yorum toplama
            last_height = self.driver.execute_script("return document.body.scrollHeight")
            scroll_count = 0
            batch_size = 20
            
            while True:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                scroll_count += 1
                time.sleep(1.2)  # Biraz daha hızlı scroll
                
                # Her batch'te yorumları parse et ve CSV'ye yaz
                if scroll_count % batch_size == 0:
                    new_count = self._parse_and_write_reviews(
                        product, seller_rate, csv_writer
                    )
                    review_count += new_count
                    csv_file.flush()  # Diske kaydet (veri kaybı önlenir)
                    
                    logger.info(f"Scroll #{scroll_count} - {review_count} yorum (+{new_count} yeni)")
                
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                
                if new_height == last_height:
                    time.sleep(1.2)
                    new_height = self.driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        # Son yorumları da al
                        new_count = self._parse_and_write_reviews(
                            product, seller_rate, csv_writer
                        )
                        review_count += new_count
                        csv_file.flush()
                        break
                
                last_height = new_height
            
            # Ürün bitince cache'i tamamen temizle
            self.processed_comments.clear()
            gc.collect()
            
            logger.info(f"Toplam {review_count} yorum çekildi")
            return review_count
            
        except Exception as e:
            logger.error(f"Yorum çekme hatası: {e}")
            return review_count
    
    def _parse_and_write_reviews(
        self, 
        product: Product, 
        seller_rate: str,
        csv_writer
    ) -> int:
        """
        Sayfadaki yorumları parse eder ve CSV'ye yazar.
        
        Returns:
            int: Yeni yazılan yorum sayısı
        """
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        reviews = soup.select('.review')
        new_count = 0
        
        for review in reviews:
            # Yorum metni
            comment_elem = review.select_one('.review-comment')
            comment = comment_elem.text.strip() if comment_elem else ""
            
            # Kullanıcı adı
            name_elem = review.select_one('.name')
            user = name_elem.text.strip() if name_elem else "Anonim"
            
            # Benzersizlik kontrolü
            unique_key = f"{user}:{comment[:50]}"
            if unique_key in self.processed_comments:
                continue
            self.processed_comments.add(unique_key)
            
            # Tarih
            date_elem = review.select_one('.date')
            if date_elem:
                date_spans = date_elem.find_all('span')
                if len(date_spans) >= 3:
                    date = f"{date_spans[0].text} {date_spans[1].text} {date_spans[2].text}"
                else:
                    date = date_elem.text.strip()
            else:
                date = "Tarih bulunamadı"
            
            # Puan
            point = self._extract_rating(review)
            
            # Satıcı
            seller_elem = review.select_one('.seller-name-wrapper strong')
            seller = seller_elem.text.strip() if seller_elem else ""
            
            # Görsel sayısı
            images = review.select('.comment-media')
            image_count = len(images)
            
            # CSV'ye yaz
            csv_writer.writerow({
                'category': product.category,
                'product_url': product.url,
                'user': user,
                'date': date,
                'point': point,
                'comment': comment,
                'seller': seller,
                'image_count': image_count,
                'seller_rate': seller_rate,
            })
            new_count += 1
        
        return new_count
    
    def _extract_rating(self, review) -> Optional[int]:
        """Yorum puanını çıkarır."""
        star_elem = review.select_one('.star-rating-full-star')
        if not star_elem:
            return None
        
        style = star_elem.get('style', '')
        if 'padding-inline-end' not in style:
            return 5
        
        try:
            padding = style.split('padding-inline-end:')[1].split('px')[0].strip()
            padding_value = float(padding)
            stars = 5 - round(padding_value / 16.7)
            return max(1, min(5, stars))
        except:
            return 5
    
    def _close_popup(self):
        """Popup kapatma helper metodu"""
        try:
            actions = ActionChains(self.driver)
            actions.move_by_offset(10, 10).click().perform()
            actions.reset_actions()
            time.sleep(2)
        except:
            pass


class TrendyolFullScraper:
    """
    Trendyol'dan kategorileri, ürünleri ve yorumları toplayan ana sınıf.
    
    Kullanım:
        scraper = TrendyolFullScraper(headless=False)
        scraper.run(
            category_indices=[1, 2, 3],  # Hangi kategoriler
            max_products_per_category=10,  # Her kategoriden kaç ürün
            output_file="yorumlar.csv"
        )
    """
    
    CSV_FIELDNAMES = [
        'category', 'product_url', 'user', 'date', 'point', 
        'comment', 'seller', 'image_count', 'seller_rate'
    ]
    
    def __init__(self, headless: bool = False):
        """
        Args:
            headless: True ise tarayıcı arka planda çalışır
        """
        self.headless = headless
        self.driver_manager = TrendyolDriver(headless)
        self.driver = None
        self.category_scraper = None
        self.review_scraper = None
        
        # İstatistikler
        self.stats = {
            'total_products': 0,
            'total_reviews': 0,
            'categories': {},
            'errors': []
        }
    
    def run(
        self,
        category_names: Optional[List[str]] = None,
        max_products_per_category: Optional[int] = None,
        output_file: Optional[str] = None,
        apply_seller_filter: bool = True
    ) -> str:
        """
        Tam scraping işlemini başlatır.
        
        Args:
            category_names: Toplanacak kategori isimleri (None = tüm kategoriler, "Popüler Ürünler" hariç)
            max_products_per_category: Her kategoriden maksimum ürün sayısı (None = tümü)
            output_file: Çıktı dosya adı (None = otomatik)
            apply_seller_filter: Satıcı filtresi uygulansın mı
            
        Returns:
            str: Çıktı dosya yolu
        """
        # Dosya adı oluştur
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"trendyol_full_reviews_{timestamp}.csv"
        
        logger.info("="*60)
        logger.info("🚀 TRENDYOL FULL SCRAPER BAŞLATILIYOR")
        logger.info(f"📁 Çıktı dosyası: {output_file}")
        logger.info("="*60)
        
        try:
            # Driver oluştur
            self.driver = self.driver_manager.create()
            self.category_scraper = TrendyolCategoryScraper(self.driver)
            self.review_scraper = TrendyolReviewScraper(self.driver)
            
            # Kategorilerden ürünleri topla
            logger.info("\n📂 ADIM 1: Kategorilerden ürünler toplanıyor...")
            products = self.category_scraper.get_all_category_products(
                category_names=category_names,
                max_products_per_category=max_products_per_category
            )
            
            if not products:
                logger.error("Hiç ürün bulunamadı!")
                return output_file
            
            self.stats['total_products'] = len(products)
            
            # Driver kontrolü - ADIM 2'ye geçmeden önce
            if not self.driver_manager.is_alive():
                logger.warning("⚠️ Driver kapanmış, yeniden başlatılıyor...")
                self.driver_manager.quit()
                self.driver = self.driver_manager.create()
                self.review_scraper = TrendyolReviewScraper(self.driver)
            
            # CSV dosyasını aç
            csv_file = open(output_file, 'w', encoding='utf-8-sig', newline='')
            csv_writer = csv.DictWriter(csv_file, fieldnames=self.CSV_FIELDNAMES)
            csv_writer.writeheader()
            csv_file.flush()
            
            # Her ürünün yorumlarını topla
            logger.info(f"\n📝 ADIM 2: {len(products)} üründen yorumlar toplanıyor...")
            
            for idx, product in enumerate(products, 1):
                # Her ürün öncesi driver kontrolü
                if not self.driver_manager.is_alive():
                    logger.warning("⚠️ Driver kapanmış, yeniden başlatılıyor...")
                    self.driver_manager.quit()
                    self.driver = self.driver_manager.create()
                    self.review_scraper = TrendyolReviewScraper(self.driver)
                
                logger.info(f"\n[{idx}/{len(products)}] 📦 {product.category}")
                logger.info(f"   URL: {product.url[:60]}...")
                
                try:
                    review_count = self.review_scraper.scrape_reviews(
                        product=product,
                        csv_writer=csv_writer,
                        csv_file=csv_file,
                        apply_seller_filter=apply_seller_filter
                    )
                    
                    product.review_count = review_count
                    self.stats['total_reviews'] += review_count
                    
                    # Kategori istatistiği
                    if product.category not in self.stats['categories']:
                        self.stats['categories'][product.category] = {
                            'products': 0,
                            'reviews': 0
                        }
                    self.stats['categories'][product.category]['products'] += 1
                    self.stats['categories'][product.category]['reviews'] += review_count
                    
                    logger.info(f"   ✅ {review_count} yorum kaydedildi")
                    
                except Exception as e:
                    error_msg = f"{product.url}: {str(e)}"
                    self.stats['errors'].append(error_msg)
                    logger.error(f"   ❌ Hata: {e}")
                
                # Ürünler arası bekleme
                if idx < len(products):
                    time.sleep(2)
            
            csv_file.close()
            
            # Sonuç raporu
            self._print_summary(output_file)
            
            return output_file
            
        finally:
            if self.driver:
                self.driver_manager.quit()
    
    def run_with_urls(
        self,
        products: List[Dict[str, str]],
        output_file: Optional[str] = None,
        apply_seller_filter: bool = True
    ) -> str:
        """
        Verilen URL listesinden yorumları toplar.
        
        Args:
            products: [{'url': '...', 'category': '...'}] formatında ürün listesi
            output_file: Çıktı dosya adı
            apply_seller_filter: Satıcı filtresi uygulansın mı
            
        Returns:
            str: Çıktı dosya yolu
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"trendyol_reviews_{timestamp}.csv"
        
        logger.info("="*60)
        logger.info("🚀 TRENDYOL YORUM ÇEKME BAŞLATILIYOR")
        logger.info(f"📁 Çıktı dosyası: {output_file}")
        logger.info(f"📦 İşlenecek ürün sayısı: {len(products)}")
        logger.info("="*60)
        
        try:
            self.driver = self.driver_manager.create()
            self.review_scraper = TrendyolReviewScraper(self.driver)
            
            # CSV dosyasını aç
            csv_file = open(output_file, 'w', encoding='utf-8-sig', newline='')
            csv_writer = csv.DictWriter(csv_file, fieldnames=self.CSV_FIELDNAMES)
            csv_writer.writeheader()
            csv_file.flush()
            
            for idx, prod_data in enumerate(products, 1):
                # Her ürün öncesi driver kontrolü
                if not self.driver_manager.is_alive():
                    logger.warning("⚠️ Driver kapanmış, yeniden başlatılıyor...")
                    self.driver_manager.quit()
                    self.driver = self.driver_manager.create()
                    self.review_scraper = TrendyolReviewScraper(self.driver)
                
                url = prod_data.get('url', '')
                category = prod_data.get('category', 'Genel')
                
                product = Product(url=url, category=category)
                
                logger.info(f"\n[{idx}/{len(products)}] 📦 {category}")
                
                try:
                    review_count = self.review_scraper.scrape_reviews(
                        product=product,
                        csv_writer=csv_writer,
                        csv_file=csv_file,
                        apply_seller_filter=apply_seller_filter
                    )
                    
                    self.stats['total_reviews'] += review_count
                    
                    if category not in self.stats['categories']:
                        self.stats['categories'][category] = {
                            'products': 0,
                            'reviews': 0
                        }
                    self.stats['categories'][category]['products'] += 1
                    self.stats['categories'][category]['reviews'] += review_count
                    
                except Exception as e:
                    self.stats['errors'].append(f"{url}: {str(e)}")
                    logger.error(f"Hata: {e}")
                
                if idx < len(products):
                    time.sleep(2)
            
            csv_file.close()
            self.stats['total_products'] = len(products)
            self._print_summary(output_file)
            
            return output_file
            
        finally:
            if self.driver:
                self.driver_manager.quit()
    
    def _print_summary(self, output_file: str):
        """Sonuç özetini yazdırır."""
        logger.info("\n" + "="*60)
        logger.info("✅ İŞLEM TAMAMLANDI!")
        logger.info("="*60)
        logger.info(f"📦 Toplam ürün: {self.stats['total_products']}")
        logger.info(f"📝 Toplam yorum: {self.stats['total_reviews']}")
        logger.info(f"📁 Dosya: {output_file}")
        
        if self.stats['categories']:
            logger.info("\n📈 KATEGORİ BAZLI ÖZET:")
            for cat, data in self.stats['categories'].items():
                logger.info(f"  • {cat}: {data['products']} ürün, {data['reviews']} yorum")
        
        if self.stats['errors']:
            logger.warning(f"\n⚠️ {len(self.stats['errors'])} hata oluştu:")
            for err in self.stats['errors'][:5]:
                logger.warning(f"  • {err[:100]}...")


class TrendyolParallelScraper:
    """
    Paralel işleme ile daha hızlı yorum çeken sınıf.
    Birden fazla worker thread kullanarak aynı anda birden fazla ürün işler.
    
    Kullanım:
        scraper = TrendyolParallelScraper(num_workers=3)
        scraper.run(products, output_file="yorumlar.csv")
    """
    
    CSV_FIELDNAMES = [
        'category', 'product_url', 'user', 'date', 'point', 
        'comment', 'seller', 'image_count', 'seller_rate'
    ]
    
    def __init__(self, num_workers: int = 3, headless: bool = False):
        """
        Args:
            num_workers: Paralel çalışacak worker sayısı (önerilen: 3-5)
            headless: True ise tarayıcılar arka planda çalışır
        """
        self.num_workers = num_workers
        self.headless = headless
        self.csv_lock = Lock()  # Thread-safe CSV yazımı için
        self.stats_lock = Lock()
        self.stats = {
            'total_products': 0,
            'processed': 0,
            'total_reviews': 0,
            'categories': {},
            'errors': []
        }
    
    def _create_worker_driver(self) -> webdriver.Chrome:
        """Worker için Chrome driver oluşturur."""
        service = Service(ChromeDriverManager().install())
        options = webdriver.ChromeOptions()
        
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--lang=tr-TR')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-infobars')
        options.add_argument('--window-size=1920,1080')
        options.add_argument(
            '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
            'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        if self.headless:
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
        
        prefs = {'profile.managed_default_content_settings.images': 2}
        options.add_experimental_option('prefs', prefs)
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
    
    def _worker_process_product(
        self, 
        product: Product, 
        csv_writer, 
        csv_file,
        worker_id: int
    ) -> int:
        """
        Tek bir ürünü işleyen worker fonksiyonu.
        Her worker kendi driver'ını oluşturur ve kapatır.
        """
        driver = None
        review_count = 0
        processed_comments: Set[str] = set()
        
        try:
            driver = self._create_worker_driver()
            logger.info(f"[Worker-{worker_id}] 🚀 {product.category} işleniyor...")
            
            # Yorum URL'sine git
            if '/yorumlar' in product.url:
                reviews_url = product.url
            else:
                if '?' in product.url:
                    base_url, query = product.url.split('?', 1)
                    reviews_url = f"{base_url}/yorumlar?{query}"
                else:
                    reviews_url = f"{product.url}/yorumlar"
            
            driver.get(reviews_url)
            time.sleep(3)
            
            # Popup kapat
            try:
                actions = ActionChains(driver)
                actions.move_by_offset(10, 10).click().perform()
                actions.reset_actions()
                time.sleep(2)
            except:
                pass
            
            # Yorumların yüklenmesini bekle
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.review, .pr-rnr-com'))
                )
            except:
                pass
            
            # Satıcı puanı
            try:
                seller_rate = driver.find_element(By.CSS_SELECTOR, '.rate-value').text
            except:
                seller_rate = "N/A"
            
            # Satıcı filtresi
            try:
                checkbox = WebDriverWait(driver, 3).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-testid="checkbox"]'))
                )
                checkbox.click()
                time.sleep(2)
            except:
                pass
            
            # Scroll ve yorum toplama
            last_height = driver.execute_script("return document.body.scrollHeight")
            scroll_count = 0
            batch_size = 20
            
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                scroll_count += 1
                time.sleep(1.2)
                
                if scroll_count % batch_size == 0:
                    new_count = self._parse_reviews_worker(
                        driver, product, seller_rate, csv_writer, processed_comments
                    )
                    review_count += new_count
                    
                    with self.csv_lock:
                        csv_file.flush()  # Diske kaydet (veri kaybı önlenir)
                    
                    logger.info(f"[Worker-{worker_id}] Scroll #{scroll_count} - {review_count} yorum")
                
                new_height = driver.execute_script("return document.body.scrollHeight")
                
                if new_height == last_height:
                    time.sleep(1.2)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    if new_height == last_height:
                        new_count = self._parse_reviews_worker(
                            driver, product, seller_rate, csv_writer, processed_comments
                        )
                        review_count += new_count
                        with self.csv_lock:
                            csv_file.flush()
                        break
                
                last_height = new_height
            
            logger.info(f"[Worker-{worker_id}] ✅ {product.category} - {review_count} yorum")
            
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] ❌ Hata: {e}")
            with self.stats_lock:
                self.stats['errors'].append(f"{product.url}: {str(e)}")
        
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            gc.collect()
        
        return review_count
    
    def _parse_reviews_worker(
        self,
        driver,
        product: Product,
        seller_rate: str,
        csv_writer,
        processed_comments: Set[str]
    ) -> int:
        """Worker için yorum parse fonksiyonu."""
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        reviews = soup.select('.review')
        new_count = 0
        
        for review in reviews:
            comment_elem = review.select_one('.review-comment')
            comment = comment_elem.text.strip() if comment_elem else ""
            
            name_elem = review.select_one('.name')
            user = name_elem.text.strip() if name_elem else "Anonim"
            
            unique_key = f"{user}:{comment[:50]}"
            if unique_key in processed_comments:
                continue
            processed_comments.add(unique_key)
            
            date_elem = review.select_one('.date')
            if date_elem:
                date_spans = date_elem.find_all('span')
                if len(date_spans) >= 3:
                    date = f"{date_spans[0].text} {date_spans[1].text} {date_spans[2].text}"
                else:
                    date = date_elem.text.strip()
            else:
                date = "Tarih bulunamadı"
            
            # Puan
            point = None
            star_elem = review.select_one('.star-rating-full-star')
            if star_elem:
                style = star_elem.get('style', '')
                if 'padding-inline-end' in style:
                    try:
                        padding = style.split('padding-inline-end:')[1].split('px')[0].strip()
                        padding_value = float(padding)
                        stars = 5 - round(padding_value / 16.7)
                        point = max(1, min(5, stars))
                    except:
                        point = 5
                else:
                    point = 5
            
            seller_elem = review.select_one('.seller-name-wrapper strong')
            seller = seller_elem.text.strip() if seller_elem else ""
            
            images = review.select('.comment-media')
            image_count = len(images)
            
            # Thread-safe CSV yazımı
            with self.csv_lock:
                csv_writer.writerow({
                    'category': product.category,
                    'product_url': product.url,
                    'user': user,
                    'date': date,
                    'point': point,
                    'comment': comment,
                    'seller': seller,
                    'image_count': image_count,
                    'seller_rate': seller_rate,
                })
            new_count += 1
        
        return new_count
    
    def run(
        self,
        products: List[Product],
        output_file: Optional[str] = None,
        apply_seller_filter: bool = True
    ) -> str:
        """
        Paralel olarak ürünleri işler.
        
        Args:
            products: İşlenecek ürün listesi
            output_file: Çıktı dosya adı
            
        Returns:
            str: Çıktı dosya yolu
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"trendyol_parallel_reviews_{timestamp}.csv"
        
        self.stats['total_products'] = len(products)
        
        logger.info("="*60)
        logger.info(f"🚀 PARALEL SCRAPER BAŞLATILIYOR ({self.num_workers} worker)")
        logger.info(f"📦 İşlenecek ürün: {len(products)}")
        logger.info(f"📁 Çıktı: {output_file}")
        logger.info("="*60)
        
        # CSV dosyasını aç
        csv_file = open(output_file, 'w', encoding='utf-8-sig', newline='')
        csv_writer = csv.DictWriter(csv_file, fieldnames=self.CSV_FIELDNAMES)
        csv_writer.writeheader()
        csv_file.flush()
        
        try:
            # ThreadPoolExecutor ile paralel işleme
            with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                futures = {}
                
                for idx, product in enumerate(products):
                    worker_id = (idx % self.num_workers) + 1
                    future = executor.submit(
                        self._worker_process_product,
                        product, csv_writer, csv_file, worker_id
                    )
                    futures[future] = product
                
                # Sonuçları topla
                for future in as_completed(futures):
                    product = futures[future]
                    try:
                        review_count = future.result()
                        
                        with self.stats_lock:
                            self.stats['processed'] += 1
                            self.stats['total_reviews'] += review_count
                            
                            if product.category not in self.stats['categories']:
                                self.stats['categories'][product.category] = {
                                    'products': 0, 'reviews': 0
                                }
                            self.stats['categories'][product.category]['products'] += 1
                            self.stats['categories'][product.category]['reviews'] += review_count
                        
                        progress = self.stats['processed']
                        total = self.stats['total_products']
                        logger.info(f"📊 İlerleme: {progress}/{total} ürün tamamlandı")
                        
                    except Exception as e:
                        logger.error(f"Future hatası: {e}")
        
        finally:
            csv_file.close()
        
        # Özet
        self._print_summary(output_file)
        return output_file
    
    def _print_summary(self, output_file: str):
        """Sonuç özetini yazdırır."""
        logger.info("\n" + "="*60)
        logger.info("✅ PARALEL İŞLEM TAMAMLANDI!")
        logger.info("="*60)
        logger.info(f"📦 Toplam ürün: {self.stats['total_products']}")
        logger.info(f"📝 Toplam yorum: {self.stats['total_reviews']}")
        logger.info(f"📁 Dosya: {output_file}")
        
        if self.stats['categories']:
            logger.info("\n📈 KATEGORİ BAZLI ÖZET:")
            for cat, data in self.stats['categories'].items():
                logger.info(f"  • {cat}: {data['products']} ürün, {data['reviews']} yorum")


def main():
    """
    Ana çalıştırma fonksiyonu.
    Örnek kullanımları içerir.
    """
    
    # ==========================================
    # SEÇENEK 1: NORMAL MOD - Tek tek ürün işle
    # ==========================================
    
    # scraper = TrendyolFullScraper(headless=False)
    # output_file = scraper.run(
    #     category_names=["Aksesuar", "Giyim", "Kozmetik & Kişisel Bakım"],
    #     max_products_per_category=None,
    #     apply_seller_filter=True
    # )
    
    # ==========================================
    # SEÇENEK 2: PARALEL MOD - Hızlı çekim (ÖNERİLEN)
    # ==========================================
    
    # Önce kategorilerden ürünleri topla
    scraper = TrendyolFullScraper(headless=False)
    scraper.driver = scraper.driver_manager.create()
    scraper.category_scraper = TrendyolCategoryScraper(scraper.driver)
    
    logger.info("📂 Kategorilerden ürünler toplanıyor...")
    products = scraper.category_scraper.get_all_category_products(
        category_names=["Aksesuar", "Giyim", "Kozmetik & Kişisel Bakım", "Kitap", "Elektronik"],
        max_products_per_category=None
    )
    
    # Driver'ı kapat (paralel scraper kendi driver'larını açacak)
    scraper.driver_manager.quit()
    
    if products:
        logger.info(f"\n🚀 {len(products)} ürün paralel olarak işlenecek...")
        
        # Paralel scraper ile yorumları çek
        # num_workers: Aynı anda kaç ürün işlensin (3-5 arası önerilen)
        parallel_scraper = TrendyolParallelScraper(
            num_workers=3,  # 3 tarayıcı aynı anda çalışır
            headless=False
        )
        
        output_file = parallel_scraper.run(products)
        print(f"\n🎉 Tüm yorumlar '{output_file}' dosyasına kaydedildi!")
    else:
        print("❌ Hiç ürün bulunamadı!")
    
    # ==========================================
    # SEÇENEK 3: Belirli URL'lerden yorum çek
    # ==========================================
    
    # products = [
    #     Product(url="https://www.trendyol.com/.../yorumlar", category="Elektronik"),
    #     Product(url="https://www.trendyol.com/.../yorumlar", category="Kozmetik"),
    # ]
    # parallel_scraper = TrendyolParallelScraper(num_workers=3)
    # output_file = parallel_scraper.run(products)


if __name__ == "__main__":
    main()
