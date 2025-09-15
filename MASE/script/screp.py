# --- SAFE PRINT / UTF-8 console ---
import sys
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

def safe_print(msg: str):
    try:
        print(msg)
    except UnicodeEncodeError:
        try:
            print(msg.encode("utf-8", "ignore").decode("utf-8", "ignore"))
        except Exception:
            print("[PRINT-ERROR]")

# -*- coding: utf-8 -*-
# NOME FILE: screp.py

import pandas as pd
import time
import random
import re
import os
from datetime import datetime
from chrome_utils import kill_zombie, new_chrome_or_exit, cleanup_profile
from selenium import webdriver
from chrome_utils import kill_zombie, new_chrome_or_exit, cleanup_profile
from selenium.webdriver.chrome.options import Options
from chrome_utils import kill_zombie, new_chrome_or_exit, cleanup_profile
from selenium.webdriver.common.by import By
from chrome_utils import kill_zombie, new_chrome_or_exit, cleanup_profile
from selenium.webdriver.support.ui import WebDriverWait
from chrome_utils import kill_zombie, new_chrome_or_exit, cleanup_profile
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import bootstrap

# === PATH da config ===
from config import MASE_INPUT_SCREP_PORTI, MASE_TEMP, MASE_CHROME_PROFILE

# --- CONFIGURAZIONE ---
FILE_INPUT_PORTI = str(MASE_INPUT_SCREP_PORTI / "porti_screp.xlsx")
PATH_FILE_TEMP   = str(MASE_TEMP)
FILE_OUTPUT_MMSI = os.path.join(PATH_FILE_TEMP, 'mmsi_attuali.csv')
BASE_URL = 'https://www.myshiptracking.com'
MAX_PAGES = 10 
def main():
    print("--- Avvio Script 1: Estrazione MMSI ---")
    options = Options()
    try:
        # profilo locale nel progetto (al posto di C:\Users\security\...)
        options.add_argument(f"user-data-dir={MASE_CHROME_PROFILE}")
        kill_zombie()
        driver, __prof = new_chrome_or_exit(headless=False)
        driver.maximize_window()
    except Exception as e:
        print(f"ERRORE: Impossibile avviare Chrome. Dettagli: {e}")
        return
        
    wait = WebDriverWait(driver, 15)

    try:
        df = pd.read_excel(FILE_INPUT_PORTI)
        df_validi = df.dropna(subset=['ID Porto'])
    except Exception as e:
        print(f"ERRORE: Impossibile leggere il file dei porti: {e}")
        cleanup_profile(driver, __prof)
        return

    risultati_navi = []
    mmsi_gia_trovati = set()

    for index, riga in df_validi.iterrows():
        nome_porto = riga['Nome Porto']
        port_id = int(riga['ID Porto'])
        print(f"\n--- Analizzando: {nome_porto} (ID: {port_id}) ---")

        # ---- PAGINAZIONE PER PORTO ----
        navi_trovate_nel_porto = 0
        page = 1
        total_pages = None  # verrà calcolato solo se leggiamo "Showing ... of N Results"

        try:
            # Carico pagina 1, provo a leggere il totale "Showing 1 - 50 of N Results"
            url_porto = f"{BASE_URL}/inport?sort=TIME&page={page}&pid={port_id}"
            driver.get(url_porto)
            time.sleep(random.uniform(0.05, 0.15))
            tabella_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "myst-table")))

            html = driver.page_source
            total_results = None
            try:
                m = re.search(r"Showing\s+\d+\s*-\s*\d+\s*of\s*(\d+)\s*Results", html, flags=re.IGNORECASE)
                if m:
                    total_results = int(m.group(1))
            except Exception:
                total_results = None

            if total_results is not None and total_results > 0:
                total_pages = min(MAX_PAGES, (total_results + 49) // 50)  # ceil(N/50)

            # Ciclo pagine: se total_pages è None, uso lo stop quando la pagina ha <50 nuove navi
            while True:
             # (per page=1 abbiamo già fatto GET sopra; per le successive rifaccio GET)
                if page > 1:
                    url_porto = f"{BASE_URL}/inport?sort=TIME&page={page}&pid={port_id}"
                    driver.get(url_porto)
                    tabella_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "myst-table")))

                soup = BeautifulSoup(tabella_element.get_attribute('outerHTML'), 'lxml')
                # NEW: conteggio righe totali in pagina, indipendente dai duplicati
                righe_totali_in_pagina = len([a for a in soup.find_all('a', href=lambda href: href and '-mmsi-' in href)])

                nuovi_in_questa_pagina = 0
                for link_nave in soup.find_all('a', href=lambda href: href and '-mmsi-' in href):
                    match = re.search(r'-mmsi-(\d+)-', link_nave['href'])
                    if match:
                        mmsi = match.group(1)
                        if mmsi not in mmsi_gia_trovati:
                            risultati_navi.append({'MMSI': mmsi, 'PORTO': nome_porto})
                            mmsi_gia_trovati.add(mmsi)
                            nuovi_in_questa_pagina += 1
                            navi_trovate_nel_porto += 1

                print(f"  -> Pagina {page}: {nuovi_in_questa_pagina} nuove / {righe_totali_in_pagina} totali.")

                # Stop conditions
                if total_pages is None:
                    # Fallback: se la pagina mostra meno di 50 righe totali, è l'ultima
                    if righe_totali_in_pagina < 50:
                        break
                else :
                    # Se abbiamo il totale ed è l'ultima pagina calcolata, usciamo
                    if page >= total_pages:
                        break
                if nuovi_in_questa_pagina == 0 and total_pages is None:
                    break


                # Prossima pagina
                page += 1
                if page > MAX_PAGES:
                    break
                time.sleep(random.uniform(0.05, 0.25))  # micro-delay tra pagine

            print(f"  -> Trovate {navi_trovate_nel_porto}  navi in totale per : {nome_porto}.")
            time.sleep(random.uniform(0.6, 1.0))  # delay tra porti

        except Exception as e:
            print(f"  -> ATTENZIONE: Errore su porto {nome_porto}. {e}")


    cleanup_profile(driver, __prof)

    if risultati_navi:
        print(f"\nScrittura di {len(risultati_navi)} MMSI unici nel file...")
        df_output = pd.DataFrame(risultati_navi)
        now = datetime.now()
        df_output['DATA ESTRAZIONE'] = now.strftime("%Y-%m-%d")
        df_output['ORA ESTRAZIONE'] = now.strftime("%H:%M:%S")
        df_output = df_output[['MMSI', 'DATA ESTRAZIONE', 'ORA ESTRAZIONE', 'PORTO']]
        import os as __os
        __tmp = str(FILE_OUTPUT_MMSI) + ".tmp"
        df_output.to_csv(__tmp, index=False)
        __os.replace(__tmp, FILE_OUTPUT_MMSI)
        print(f"Salvataggio completato! File aggiornato: {FILE_OUTPUT_MMSI}")
    else:
        print("\nNessun MMSI trovato.")
    
    print("--- FINE SCRIPT 1 ---")

if __name__ == '__main__':
    main()
