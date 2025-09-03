
import os, sys, tempfile, shutil, subprocess, time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service

def kill_zombie():
    try:
        subprocess.run(["taskkill", "/F", "/IM", "chromedriver.exe"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass
    try:
        subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass

def new_chrome_or_exit(headless: bool = True):
    user_dir = os.path.join(tempfile.gettempdir(), f"mase_chrome_{os.getpid()}_{int(time.time())}")
    os.makedirs(user_dir, exist_ok=True)

    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-first-run")
    opts.add_argument("--no-default-browser-check")
    opts.add_argument("--user-data-dir=" + user_dir)
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-extensions")

    try:
        driver = webdriver.Chrome(service=Service(), options=opts)
        driver.set_page_load_timeout(60)
        return driver, user_dir
    except Exception as e:
        print(f"ERRORE: Impossibile avviare Chrome. Dettagli: {e}")
        try:
            shutil.rmtree(user_dir, ignore_errors=True)
        except Exception:
            pass
        sys.exit(12)

def cleanup_profile(driver, user_dir: str):
    try:
        driver.quit()
    except Exception:
        pass
    try:
        shutil.rmtree(user_dir, ignore_errors=True)
    except Exception:
        pass
