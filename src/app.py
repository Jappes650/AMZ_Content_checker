#!/usr/bin/env python3
# coding: utf-8

import os, sys, time, random, pickle, re, json, threading, ssl
from urllib.parse import urlparse
from urllib.request import urlopen, Request
from io import BytesIO
from tkinter import *
from tkinter import messagebox, filedialog, ttk

import pandas as pd
import xlsxwriter
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from PIL import Image  # Pillow

# ================= Excel/Bild-Dimensionierung =================
EXCEL_DPI = 96
TARGET_INCH = 1.0
TARGET_PX = int(TARGET_INCH * EXCEL_DPI)  # ~96 px
IMAGE_SIZE_MODE = "fit_to_cell"           # "fit_to_cell" oder "percent"
IMAGE_PERCENT = 0.15

def pixels_to_col_width(pixels: int) -> float:
    return max(1.0, (pixels - 5) / 7.0)

def calc_image_scale_for_cell(img_w: int, img_h: int, cell_w_px: int, cell_h_px: int):
    if img_w <= 0 or img_h <= 0:
        return 1.0, 1.0
    s = min(cell_w_px / float(img_w), cell_h_px / float(img_h), 1.0)
    return s, s

# ================= Utilities =================
ssl._create_default_https_context = ssl._create_unverified_context
_ILLEGAL_XML_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F]')
ICON_PAT = re.compile(r'(360|sprite|immersive|video|play|turntable|spin)', re.IGNORECASE)

def sanitize_excel_text(s) -> str:
    if s is None: return ""
    s = str(s)
    s = _ILLEGAL_XML_RE.sub('', s)
    return s[:32767]

def is_dot_decimal_domain(domain: str) -> bool:
    return domain.endswith("amazon.com") or domain.endswith("amazon.co.uk")

def strip_query(u: str) -> str:
    return u.split("?", 1)[0]

def remove_amazon_size_suffix(fname: str) -> str:
    # "81abcXYZL._AC_UX679_.jpg" -> "81abcXYZL.jpg"
    return re.sub(r'\._[^.]+(?=\.(?:jpg|jpeg|png|gif)$)', '', fname, flags=re.IGNORECASE)

def to_hi_res_amazon_url(url: str) -> str:
    try:
        base = strip_query(url)
        parts = urlparse(base)
        path = remove_amazon_size_suffix(parts.path)
        return f"{parts.scheme}://{parts.netloc}{path}"
    except Exception:
        return strip_query(url)

def normalize_url(url: str) -> str:
    try:
        u = to_hi_res_amazon_url(url)
        u = re.sub(r'^https://([^/]+)/', lambda m: f"https://{m.group(1).lower()}/", u)
        return u
    except Exception:
        return url

def url_dedupe_key(u: str) -> str:
    nu = normalize_url(u)
    base = os.path.basename(urlparse(nu).path).lower()
    return remove_amazon_size_suffix(base)

def looks_like_icon(url: str) -> bool:
    return bool(ICON_PAT.search(url))

def download_image_safe(url: str, timeout: int = 12):
    """-> (BytesIO, filename, (w,h)) | (None, None, (0,0))"""
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
        if not data or len(data) < 16:
            return None, None, (0, 0)

        bio = BytesIO(data)
        try:
            img = Image.open(bio)
            fmt = (img.format or "").upper()
            w, h = img.size
        except Exception:
            return None, None, (0, 0)

        if fmt not in ("JPEG", "PNG"):
            try:
                out = BytesIO()
                if img.mode in ("RGBA", "LA", "P"): img = img.convert("RGBA")
                else: img = img.convert("RGB")
                img.save(out, format="PNG", optimize=True)
                out.seek(0)
                return out, "image.png", img.size
            except Exception:
                return None, None, (0, 0)

        ext = "jpg" if fmt == "JPEG" else "png"
        bio.seek(0)
        return bio, f"image.{ext}", (w, h)
    except Exception:
        return None, None, (0, 0)

def extract_price(soup: BeautifulSoup, domain: str) -> str:
    # 1) Bevorzugt a-offscreen (kompletter Preis inkl. Fraction)
    selectors = [
        '#corePrice_feature_div span.a-offscreen',
        '.reinventPricePriceToPayMargin span.a-offscreen',
        'span.a-price span.a-offscreen',
        'span.a-offscreen',
        '#priceblock_ourprice',
        '#priceblock_dealprice',
    ]
    for sel in selectors:
        el = soup.select_one(sel)
        if el:
            t = el.get_text(strip=True)
            if t: return sanitize_excel_text(t)

    # 2) Fallback: whole + fraction zusammensetzen
    whole_el = soup.select_one('span.a-price span.a-price-whole') or soup.select_one('span.a-price-whole')
    frac_el  = soup.select_one('span.a-price span.a-price-fraction') or soup.select_one('span.a-price-fraction')
    if whole_el:
        whole_txt = re.sub(r'\D', '', whole_el.get_text())
        frac_txt  = re.sub(r'\D', '', (frac_el.get_text() if frac_el else "00"))
        sep = '.' if is_dot_decimal_domain(domain) else ','
        if frac_txt == "": frac_txt = "00"
        return f"{whole_txt}{sep}{frac_txt}"
    return "Preis nicht verfügbar"

def parse_background_url(style_value: str) -> str:
    if not style_value: return ""
    m = re.compile(r'url\((?:\"|\')?(.*?)(?:\"|\')?\)').search(style_value)
    return m.group(1) if m else ""

# ================= Bild-Extraktion ausschließlich aus <div class="ivRow"> =================
def extract_image_urls(soup: BeautifulSoup, want: int = 50) -> list:
    """
    Liest nur die Galerie aus allen <div class="ivRow">:
      - In jeder Row: alle <div class="ivThumb">,
      - Bild-URL aus innerem <div class="ivThumbImage" style="background: url(...)" />
      - Reihenfolge per id="ivImage_X" (fallback data-csa-c-posy)
    Ergebnis: [Bild_0 (Hauptbild), Bild_1, Bild_2, ...]
    """
    items = []  # (pos, url)
    for row in soup.select("div.ivRow"):
        for thumb in row.select("div.ivThumb"):
            # Position bestimmen
            id_attr = thumb.get("id", "")
            m = re.search(r"ivImage_(\d+)", id_attr)
            if m:
                pos = int(m.group(1))
            else:
                try:
                    pos = int(thumb.get("data-csa-c-posy", "9999"))
                except ValueError:
                    pos = 9999

            # URL aus style
            img_div = thumb.select_one("div.ivThumbImage[style]")
            if not img_div:
                continue
            src = parse_background_url(img_div.get("style"))
            if not src:
                continue
            items.append((pos, src))

    # sortieren & normalisieren & deduplizieren
    items.sort(key=lambda t: t[0])
    final = []
    seen_keys = set()
    for pos, u in items:
        if looks_like_icon(u):
            continue
        nu = normalize_url(u)
        k = url_dedupe_key(nu)
        if k in seen_keys:
            continue
        seen_keys.add(k)
        final.append(nu)

    return final[:want]

# ================== Galerie wirklich laden (Klick + wait & scroll) ==================
def click_main_image_to_init_gallery(driver):
    """
    Klickt das Hauptbild/den ImageBlock an, damit Amazon den Immersive-View/ivRow rendert.
    """
    selectors = [
        "#imageBlock_feature_div img#landingImage",
        "#imageBlock_feature_div #imgTagWrapperId img",
        "#imageBlock_feature_div img",
        "#imgTagWrapperId img",
        "#main-image-container img",
        "#imageBlock_feature_div",
    ]
    for sel in selectors:
        try:
            elem = WebDriverWait(driver, 3).until(
                lambda d: d.find_element("css selector", sel)
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)
            time.sleep(0.4)
            try:
                elem.click()
            except Exception:
                # fallback: JS-click
                driver.execute_script("arguments[0].click();", elem)
            return True
        except Exception:
            continue
    return False

def ensure_gallery_loaded(driver, max_wait=10, scroll_tries=4):
    """
    Sorgt dafür, dass die ivRow/ivThumbImage-Elemente im DOM vorhanden sind:
    - scrollt den Galerieblock in den Viewport
    - macht leichte Scroll-Bewegungen, um Lazy-Load zu triggern
    - wartet gezielt auf 'div.ivRow div.ivThumbImage'
    """
    try:
        driver.execute_script("""
            var el = document.querySelector('#altImages') 
                  || document.querySelector('#imageBlock') 
                  || document.querySelector('#imageBlock_feature_div') 
                  || document.querySelector('div.ivRow');
            if (el) { el.scrollIntoView({block: 'center', inline: 'nearest'}); }
        """)
        time.sleep(0.4)
    except Exception:
        pass

    for _ in range(scroll_tries):
        try:
            WebDriverWait(driver, 2).until(
                lambda d: d.find_elements("css selector", "div.ivRow div.ivThumbImage")
            )
            return True
        except TimeoutException:
            try:
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(0.25)
                driver.execute_script("window.scrollBy(0, -350);")
                time.sleep(0.25)
            except Exception:
                time.sleep(0.25)

    try:
        WebDriverWait(driver, max_wait).until(
            lambda d: d.find_elements("css selector", "div.ivRow div.ivThumbImage")
        )
        return True
    except TimeoutException:
        return False

def update_progress_display(self, asin, prozent, status="erfolgreich"):
    self.output.insert(END, f"ASIN: {asin} {prozent} {status}!\n")
    self.output.see("end")

# ================= GUI / Main =================
class AmazonImageScraper:
    def __init__(self, master):
        self.master = master
        master.title("Amazon Image Scraper – Multi-Country (PLZ & Buybox Fix)")
        master.geometry("560x500")

        self.countries = {
            "Deutschland (DE)": {"code": "de", "domain": "amazon.de"},
            "Frankreich (FR)": {"code": "fr", "domain": "amazon.fr"},
            "Spanien (ES)": {"code": "es", "domain": "amazon.es"},
            "Schweden (SE)": {"code": "se", "domain": "amazon.se"},
            "Niederlande (NL)": {"code": "nl", "domain": "amazon.nl"},
            "Polen (PL)": {"code": "pl", "domain": "amazon.pl"},
            "Italien (IT)": {"code": "it", "domain": "amazon.it"},
            "Großbritannien (UK)": {"code": "uk", "domain": "amazon.co.uk"},
            "USA (US)": {"code": "us", "domain": "amazon.com"},
            "Belgien (BE)": {"code": "be", "domain": "amazon.com.be"},
        }

        frame = Frame(); frame.pack(pady=10)
        country_frame = Frame(frame); country_frame.pack(pady=5)
        Label(country_frame, text="Land auswählen:").pack(side=LEFT, padx=5)
        self.country_var = StringVar()
        self.country_combobox = ttk.Combobox(country_frame, textvariable=self.country_var,
                                             values=list(self.countries.keys()), state="readonly", width=26)
        self.country_combobox.set("Deutschland (DE)"); self.country_combobox.pack(side=LEFT, padx=5)

        # Login
        self.button_login = Button(frame, text="Amazon Login", command=self.login_func, bg='blue', fg='white')
        self.button_login.pack(side=TOP, pady=5)
        self.login_status = Label(frame, text="Status: Nicht angemeldet", fg='red')
        self.login_status.pack(side=TOP, pady=5)

        # Performance/Optionen
        perf_frame = Frame(frame); perf_frame.pack(pady=5)
        Label(perf_frame, text="Min Pause (Sek):").grid(row=0, column=0, padx=2, sticky=E)
        self.min_pause_var = StringVar(value="2")
        Entry(perf_frame, textvariable=self.min_pause_var, width=6).grid(row=0, column=1, padx=2)

        Label(perf_frame, text="Max Pause (Sek):").grid(row=0, column=2, padx=2, sticky=E)
        self.max_pause_var = StringVar(value="5")
        Entry(perf_frame, textvariable=self.max_pause_var, width=6).grid(row=0, column=3, padx=2)

        self.headless_var = BooleanVar(value=True)
        Checkbutton(perf_frame, text="Headless (schneller)", variable=self.headless_var)\
            .grid(row=1, column=0, columnspan=2, sticky=W, padx=2)

        Label(perf_frame, text="Startspalten (min):").grid(row=1, column=2, padx=2, sticky=E)
        self.min_image_cols_var = StringVar(value="9")
        Entry(perf_frame, textvariable=self.min_image_cols_var, width=6).grid(row=1, column=3, padx=2)

        # Neue Eingabe: Liefer-PLZ
        plz_frame = Frame(frame); plz_frame.pack(pady=5)
        Label(plz_frame, text="Liefer-PLZ:").grid(row=0, column=0, padx=5, sticky=E)
        self.zip_var = StringVar(value="")
        Entry(plz_frame, textvariable=self.zip_var, width=12).grid(row=0, column=1, padx=5, sticky=W)
        Label(plz_frame, text="(wird vor der 1. Suche angewendet)").grid(row=0, column=2, padx=5, sticky=W)

        # Datei Button
        self.button1 = Button(frame, text="ASIN.csv Liste öffnen", command=self.start_scraping, bg='#0a7', fg='white')
        self.button1.pack(side=TOP, pady=10)

        self.progress = ttk.Progressbar(master, length=520, mode='determinate'); self.progress.pack(pady=5)

        scrollbar = Scrollbar(master); scrollbar.pack(side=RIGHT, fill=Y)
        self.output = Text(master, width="100", height="16", background='black', fg='lime',
                           yscrollcommand=scrollbar.set)
        self.output.pack(side=LEFT, fill=BOTH, expand=True)

        self.cookie_file = "amazon_cookies.pkl"
        self.is_logged_in = False
        self.current_domain = "amazon.de"

    def get_selected_domain(self):
        sel = self.country_var.get()
        return self.countries.get(sel, {"domain":"amazon.de"})["domain"]

    def get_random_user_agent(self):
        return random.choice([
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15"
        ])

    def setup_chrome_options(self, headless=None):
        if headless is None: headless = self.headless_var.get()
        opts = Options()
        opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu"); opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option('useAutomationExtension', False)
        # Browser-Bilder aus (wir laden separat):
        opts.add_argument("--blink-settings=imagesEnabled=false")
        user_data_dir = os.path.join(os.getcwd(), f"chrome_user_data_{self.get_selected_domain().replace('.', '_')}")
        opts.add_argument(f"--user-data-dir={user_data_dir}")
        opts.add_argument(f"--user-agent={self.get_random_user_agent()}")
        if headless: opts.add_argument("--headless=new")
        return opts

    def save_cookies(self, driver):
        try:
            domain = self.get_selected_domain()
            cookie_file = f"amazon_cookies_{domain.replace('.', '_')}.pkl"
            with open(cookie_file, 'wb') as f:
                pickle.dump(driver.get_cookies(), f)
            self.update_output("✅ Cookies gespeichert")
        except Exception as e:
            self.update_output(f"❌ Fehler beim Speichern der Cookies: {e}")

    def load_cookies(self, driver):
        try:
            domain = self.get_selected_domain()
            cookie_file = f"amazon_cookies_{domain.replace('.', '_')}.pkl"
            if os.path.exists(cookie_file):
                with open(cookie_file, 'rb') as f:
                    cookies = pickle.load(f)
                driver.get(f"https://www.{domain}"); time.sleep(2)
                for c in cookies:
                    try: driver.add_cookie(c)
                    except Exception: continue
                driver.refresh()
                self.update_output("✅ Cookies geladen"); return True
        except Exception as e:
            self.update_output(f"❌ Fehler beim Laden der Cookies: {e}")
        return False

    def check_login_status(self, driver):
        try:
            domain = self.get_selected_domain()
            driver.get(f"https://www.{domain}/gp/css/homepage.html"); time.sleep(3)
            page = driver.page_source.lower()
            return any(k in page for k in ["sign out","abmelden","bestellungen","your orders"])
        except Exception as e:
            self.update_output(f"❌ Fehler bei Login-Status-Prüfung: {e}")
            return False

    def create_safe_excel_path(self, base_path):
        try:
            directory = os.path.dirname(base_path); os.makedirs(directory, exist_ok=True)
            base_name = "Amazon_Output"; ext = ".xlsx"
            for i in range(1, 100):
                name = f"{base_name}{ext}" if i == 1 else f"{base_name}_{i}{ext}"
                p = os.path.join(directory, name)
                if not os.path.exists(p): return p
            desktop = os.path.expanduser("~/Desktop")
            return os.path.join(desktop, f"Amazon_Output_{int(time.time())}.xlsx")
        except Exception:
            desktop = os.path.expanduser("~/Desktop")
            return os.path.join(desktop, f"Amazon_Output_{int(time.time())}.xlsx")

    def amazon_login(self):
        domain = self.get_selected_domain()
        opts = self.setup_chrome_options(headless=False)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=opts)
        try:
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            if self.load_cookies(driver) and self.check_login_status(driver):
                self.update_output("✅ Bereits mit gespeicherten Cookies angemeldet!")
                self.is_logged_in = True
                self.login_status.config(text=f"Status: Angemeldet ({domain}) ✅", fg='green')
                driver.quit(); return True

            login_url = (f"https://www.{domain}/ap/signin"
                         f"?openid.pape.max_auth_age=900"
                         f"&openid.return_to=https%3A%2F%2Fwww.{domain}%2Fgp%2Fyourstore%2Fhome%3F"
                         f"language%3Den%26path%3D%252Fgp%252Fyourstore%252Fhome%26signIn%3D1"
                         f"%26useRedirectOnSuccess%3D1%26action%3Dsign-out%26ref_%3Dnav_AccountFlyout_signout"
                         f"&language=en"
                         f"&openid.assoc_handle=deflex"
                         f"&openid.mode=checkid_setup"
                         f"&openid.ns=http%3A%2F%2Fspecs.openid.net%2Fauth%2F2.0")
            self.update_output(f"🔍 Öffne Login: {domain}")
            driver.get(login_url); time.sleep(3)
            self.update_output("🔒 Bitte im Browser einloggen und dann 'Login abschließen' klicken.")
            self.button_login.config(state='disabled')
            complete_button = Button(self.button_login.master, text="Login abschließen",
                                     command=lambda: self.complete_login(driver), bg='green', fg='white')
            complete_button.pack(pady=5); self.complete_button = complete_button
        except Exception as e:
            self.update_output(f"❌ Login-Fehler: {e}"); driver.quit(); return False

    def complete_login(self, driver):
        try:
            if self.check_login_status(driver):
                domain = self.get_selected_domain()
                self.update_output("✅ Login erfolgreich!")
                self.save_cookies(driver); self.is_logged_in = True
                self.login_status.config(text=f"Status: Angemeldet ({domain}) ✅", fg='green')
                self.button_login.config(state='normal')
                if hasattr(self, 'complete_button'): self.complete_button.destroy()
                driver.quit(); return True
            else:
                self.update_output("❌ Login noch nicht vollständig.")
                return False
        except Exception as e:
            self.update_output(f"❌ Fehler beim Abschließen: {e}")
            self.button_login.config(state='normal')
            if hasattr(self, 'complete_button'): self.complete_button.destroy()
            driver.quit(); return False

    # --------- NEU: Liefer-PLZ anwenden, bevor erste Suche startet ---------
    def apply_zip_before_search(self, driver):
        zip_code = (self.zip_var.get() or "").strip()
        if not zip_code:
            self.update_output("ℹ️ Keine Liefer-PLZ gesetzt – überspringe Standortanpassung.")
            return

        domain = self.get_selected_domain()
        self.update_output(f"📍 Setze Liefer-PLZ '{zip_code}' auf {domain}...")

        try:
            # Homepage laden (mit sichtbaren Bildern, sonst sind Popover-Assets evtl. nicht da)
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": self.get_random_user_agent()})
            driver.get(f"https://www.{domain}/?language=en")
            WebDriverWait(driver, 15).until(lambda d: d.execute_script("return document.readyState")=="complete")

            # Popover öffnen
            link = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#nav-global-location-popover-link"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", link)
            time.sleep(0.3)
            try:
                link.click()
            except Exception:
                driver.execute_script("arguments[0].click();", link)

            # Input finden & PLZ setzen
            zip_input = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "#GLUXZipUpdateInput"))
            )
            zip_input.clear()
            zip_input.send_keys(zip_code)

            # Apply klicken
            apply_btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "#GLUXZipUpdate"))
            )
            try:
                apply_btn.click()
            except Exception:
                driver.execute_script("arguments[0].click();", apply_btn)

            # Kurzes Warten, bis Standort übernommen wird
            time.sleep(2.0)
            self.update_output("✅ Liefer-PLZ angewendet.")

        except Exception as e:
            self.update_output(f"⚠️ Konnte Liefer-PLZ nicht setzen: {e}. Fahre ohne fort.")

    def scrape_product_data(self):
        if not self.is_logged_in:
            messagebox.showerror("Fehler", "Bitte melden Sie sich zuerst bei Amazon an!")
            return

        import_file_path = filedialog.askopenfilename(
            title="ASIN CSV-Datei auswählen", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
        )
        if not import_file_path: return

        workbook = None; output_path = None; driver = None

        try:
            min_pause = float(self.min_pause_var.get()); max_pause = float(self.max_pause_var.get())
            min_cols  = int(self.min_image_cols_var.get())
            domain = self.get_selected_domain()

            try:
                data = pd.read_csv(import_file_path, header=None)
                self.update_output(f"CSV geladen: {len(data)} ASINs gefunden")
            except Exception as csv_error:
                self.update_output(f"CSV-Fehler: {csv_error}"); return

            totalrows = len(data); self.progress['maximum'] = totalrows

            directory = os.path.dirname(import_file_path)
            output_path = self.create_safe_excel_path(os.path.join(directory, "Amazon_Output.xlsx"))
            workbook = xlsxwriter.Workbook(output_path, {'strings_to_numbers': False})
            worksheet = workbook.add_worksheet("Produktdaten")

            headers = ['ASIN', 'Titel', 'Preis', 'Verkäufer', 'Buybox Status']
            for c,h in enumerate(headers): worksheet.write(0, c, h)
            worksheet.set_column(0,0,11); worksheet.set_column(1,1,50)
            worksheet.set_column(2,2,16); worksheet.set_column(3,3,22); worksheet.set_column(4,4,18)

            row = 1; success = 0
            opts = self.setup_chrome_options()
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=opts)
            try:
                driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                self.load_cookies(driver)

                # >>> Neu: Liefer-PLZ setzen, BEVOR die erste Produktseite geladen wird
                self.apply_zip_before_search(driver)

                for i, asin_raw in enumerate(data.iloc[:,0]):
                    try:
                        self.progress['value'] = i; self.master.update()
                        asin = str(asin_raw).strip().upper()
                        if not asin: continue

                        self.update_output(f"Verarbeite ASIN {i+1}/{totalrows}: {asin}")

                        try:
                            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": self.get_random_user_agent()})
                        except Exception: pass

                        ok = False
                        for attempt in range(2):
                            try:
                                url = f"https://www.{domain}/dp/{asin}/"
                                self.update_output(f"   Versuch {attempt+1}: {url}")
                                driver.get(url)
                                WebDriverWait(driver, 12).until(lambda d: d.execute_script("return document.readyState")=="complete")
                                time.sleep(random.uniform(1.0, 2.0))
                                if "/dp/" in driver.current_url and "productTitle" in driver.page_source:
                                    ok = True; self.update_output("   ✅ Seite geladen"); break
                                else:
                                    self.update_output("   Kein Produktinhalt")
                            except TimeoutException:
                                self.update_output("   Timeout"); continue
                        if not ok:
                            self.update_output(f"   ❌ ASIN {asin} konnte nicht geladen werden"); continue

                        # ======= Hauptbild anklicken (Immersive View/Galerie initialisieren) =======
                        if click_main_image_to_init_gallery(driver):
                            self.update_output("   🔎 Hauptbild angeklickt, um Galerie zu laden...")
                            time.sleep(1.2)
                        else:
                            self.update_output("   ⚠️ Hauptbild nicht klickbar – versuche dennoch, Galerie zu laden")

                        # ======= Galerie wirklich laden (Lazy-Load triggern) =======
                        gallery_ok = ensure_gallery_loaded(driver, max_wait=10, scroll_tries=4)
                        if not gallery_ok:
                            self.update_output("   ⚠️ Galerie (ivRow) nicht sicher geladen – fahre dennoch fort")

                        # jetzt frisches HTML holen
                        html = driver.page_source
                        soup = BeautifulSoup(html, "html.parser")
                        self.update_output("   Extrahiere Produktdaten...")

                        # Titel
                        title_el = (
                            soup.find('span', id="productTitle") or
                            soup.find('h1', class_="a-size-large product-title-word-break") or
                            soup.find('span', class_="a-size-large product-title-word-break")
                        )
                        title = title_el.get_text(strip=True) if title_el else "Titel nicht gefunden"

                        # Preis
                        price = extract_price(soup, domain)

                        # Verkäufer (leer, wenn nicht gefunden)
                        seller_el = (
                            soup.find('span', {'class': 'a-size-small mbcMerchantName'}) or
                            soup.find('a', {'id':'sellerProfileTriggerId'}) or
                            soup.find('div', {'id':'merchant-info'})
                        )
                        seller = seller_el.get_text(strip=True) if seller_el else ""

                        # Buybox-Entscheidung NUR über Verkäufer-Feld:
                        # - Verkäufer leer  -> Nicht Qualifiziert
                        # - Verkäufer gesetzt -> Qualifiziert
                        buybox = "Qualifiziert" if seller else "Nicht Qualifiziert"

                        # Schreiben Basisdaten
                        worksheet.write(row, 0, sanitize_excel_text(asin))
                        worksheet.write(row, 1, sanitize_excel_text(title))
                        worksheet.write(row, 2, sanitize_excel_text(price))
                        worksheet.write(row, 3, sanitize_excel_text(seller))
                        worksheet.write(row, 4, sanitize_excel_text(buybox))

                        # --------- Bilder (nur ivRow) ---------
                        urls = extract_image_urls(soup, want=50)
                        self.update_output(f"   Galerie-URLs aus ivRow (dedupl.): {len(urls)}")

                        col_offset = 5
                        needed_cols = max(min_cols, len(urls))
                        # Header + Spaltenbreite — dynamisch erweitern
                        if row == 1:
                            for h in range(needed_cols):
                                worksheet.write(0, col_offset + h, f'Bild {h+1}')
                                worksheet.set_column(col_offset + h, col_offset + h, pixels_to_col_width(TARGET_PX))
                        else:
                            for h in range(needed_cols):
                                worksheet.set_column(col_offset + h, col_offset + h, pixels_to_col_width(TARGET_PX))

                        worksheet.set_row(row, TARGET_INCH * 72)

                        slot = 0
                        for u in urls:
                            hi = to_hi_res_amazon_url(u)
                            img_stream, fname, (w,h) = download_image_safe(hi)
                            if not img_stream:
                                continue
                            # sehr kleine Icons/Badges raus (z.B. 125×125 360°-Icon)
                            if w <= 130 and h <= 130:
                                continue

                            img_stream.seek(0)
                            if IMAGE_SIZE_MODE == "percent":
                                x_scale = y_scale = IMAGE_PERCENT
                            else:
                                x_scale, y_scale = calc_image_scale_for_cell(w, h, TARGET_PX, TARGET_PX)

                            worksheet.insert_image(
                                row, col_offset + slot, fname,
                                {'image_data': img_stream, 'x_scale': x_scale, 'y_scale': y_scale,
                                 'x_offset': 2, 'y_offset': 2, 'object_position': 1}
                            )
                            slot += 1

                        self.update_output(f"   ✅ Bilder eingefügt: {slot}")

                        success += 1
                        perc = f"{round((success/totalrows)*100)}%"; update_progress_display(self, asin, perc)
                        row += 1

                        time.sleep(random.uniform(min_pause, max_pause))

                    except Exception as e:
                        self.update_output(f"   ❌ Fehler bei ASIN {asin}: {str(e)[:200]}")
                        continue

                self.progress['value'] = totalrows; self.master.update()

            finally:
                try:
                    if driver is not None: driver.quit()
                except Exception: pass

                if workbook is not None:
                    try:
                        workbook.close()
                        self.update_output(f"✅ Excel-Datei gespeichert: {output_path}")
                    except Exception as e:
                        self.update_output(f"❌ Fehler beim Schließen der Excel-Datei: {e}")

            self.update_output("✅ Scraping abgeschlossen!")
            self.update_output(f"   Land: {domain}")
            self.update_output(f"   Verarbeitet: {success}/{totalrows} ASINs")
            self.update_output(f"   Datei: {output_path}")

            messagebox.showinfo("Erfolg",
                f"Scraping abgeschlossen!\n\nLand: {domain}\nVerarbeitet: {success}/{totalrows} ASINs\n"
                f"Datei gespeichert:\n{output_path}"
            )

        except Exception as e:
            self.update_output(f"❌ Allgemeiner Fehler: {e}")
            messagebox.showerror("Fehler", f"Ein Fehler ist aufgetreten:\n{e}")
        finally:
            self.progress['value'] = 0

    def login_func(self):
        def t():
            try: self.amazon_login()
            except Exception as e: self.update_output(f"❌ Login-Thread Fehler: {e}")
        threading.Thread(target=t, daemon=True).start()

    def start_scraping(self):
        def t():
            try: self.scrape_product_data()
            except Exception as e: self.update_output(f"❌ Scraping-Thread Fehler: {e}")
        threading.Thread(target=t, daemon=True).start()

    def update_output(self, msg):
        try:
            self.output.insert(END, msg + "\n"); self.output.see(END); self.master.update_idletasks()
        except Exception: pass

if __name__ == "__main__":
    try:
        root = Tk(); app = AmazonImageScraper(root)
        def on_closing():
            try: root.quit(); root.destroy()
            except: pass
            finally: sys.exit(0)
        root.protocol("WM_DELETE_WINDOW", on_closing)
        root.mainloop()
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        print(f"Startup Fehler: {e}"); sys.exit(1)
