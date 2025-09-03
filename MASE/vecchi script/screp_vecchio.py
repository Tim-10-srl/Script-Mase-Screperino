# -*- coding: utf-8 -*-
# NOME FILE: screp.py

import pandas as pd
import time
import re
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

# --- CONFIGURAZIONE ---
FILE_INPUT_PORTI = r'C:\Users\security\Documents\Codice\Python\MASE\Input\Screp\Porti\porti_screp.xlsx'
PATH_FILE_TEMP = r'C:\Users\security\Documents\Codice\Python\MASE\Input\File_Temp'
FILE_OUTPUT_MMSI = os.path.join(PATH_FILE_TEMP, 'mmsi_attuali.csv')
BASE_URL = 'https://www.myshiptracking.com'

def main():
    print("--- Avvio Script 1: Estrazione MMSI ---")
    options = Options()
    try:
        # Usa un profilo di test per memorizzare i cookie e partire in automatico
        options.add_argument(r"user-data-dir=C:\Users\security\chrome-test-profile")
        driver = webdriver.Chrome(options=options)
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
        driver.quit()
        return

    risultati_navi = []
    mmsi_gia_trovati = set()

    for index, riga in df_validi.iterrows():
        nome_porto = riga['Nome Porto']
        port_id = int(riga['ID Porto'])
        print(f"\n--- Analizzando: {nome_porto} (ID: {port_id}) ---")
        url_porto = f"{BASE_URL}/?port={port_id}"
        try:
            driver.get(url_porto)
            details_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "info_details")))
            relative_link = details_element.get_attribute('href')
            full_url = BASE_URL + relative_link
            driver.get(full_url)
            tabella_element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "myst-table")))
            soup = BeautifulSoup(tabella_element.get_attribute('outerHTML'), 'lxml')
            
            navi_trovate_nel_porto = 0
            for link_nave in soup.find_all('a', href=lambda href: href and '-mmsi-' in href):
                match = re.search(r'-mmsi-(\d+)-', link_nave['href'])
                if match:
                    mmsi = match.group(1)
                    if mmsi not in mmsi_gia_trovati:
                        risultati_navi.append({'MMSI': mmsi, 'PORTO': nome_porto})
                        mmsi_gia_trovati.add(mmsi)
                        navi_trovate_nel_porto += 1
            print(f"  -> Trovate {navi_trovate_nel_porto} nuove navi.")
        except Exception as e:
            print(f"  -> ATTENZIONE: Errore su porto {nome_porto}. {e}")
        time.sleep(0.2)

    driver.quit()

    if risultati_navi:
        print(f"\nScrittura di {len(risultati_navi)} MMSI unici nel file...")
        df_output = pd.DataFrame(risultati_navi)
        now = datetime.now()
        df_output['DATA ESTRAZIONE'] = now.strftime("%Y-%m-%d")
        df_output['ORA ESTRAZIONE'] = now.strftime("%H:%M:%S")
        df_output = df_output[['MMSI', 'DATA ESTRAZIONE', 'ORA ESTRAZIONE', 'PORTO']]
        df_output.to_csv(FILE_OUTPUT_MMSI, index=False)
        print(f"Salvataggio completato! File aggiornato: {FILE_OUTPUT_MMSI}")
    else:
        print("\nNessun MMSI trovato.")
    
    print("--- FINE SCRIPT 1 ---")

if __name__ == '__main__':
    main()