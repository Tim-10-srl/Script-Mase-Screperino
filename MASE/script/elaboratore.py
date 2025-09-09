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
# NOME FILE: elaboratore.py

import pandas as pd
import time
import re
import os
import shutil
from datetime import datetime
from bs4 import BeautifulSoup
import random
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
import bootstrap

# === PATH da config ===
from config import MASE_TEMP, MASE_OUTPUT_NAVI, MASE_CHROME_PROFILE

# --- CONFIGURAZIONE ---
PATH_FILE_TEMP = str(MASE_TEMP)
FILE_PRECEDENTE = os.path.join(PATH_FILE_TEMP, 'mmsi_precedenti.csv')
FILE_ATTUALE   = os.path.join(PATH_FILE_TEMP, 'mmsi_attuali.csv')
PATH_OUTPUT    = str(MASE_OUTPUT_NAVI)
BASE_URL       = 'https://www.myshiptracking.com'
MAX_RETRY = 6  # massimo tentativi di retry per MMSI; oltre questa soglia non si riprova

def separa_data_ora_e_formatta(datetime_str):
    """
    Funzione robusta che separa data/ora e formatta la data in gg/mm/aaaa.
    Gestisce sia 'YYYY-MM-DD HH:MM' che 'YYYY-MM-DDHH:MM'.
    """
    if pd.isna(datetime_str) or not isinstance(datetime_str, str):
        return 'NO DATA', 'NO DATA'
    try:
        clean_str = re.sub('<[^<]+?>', '', datetime_str).strip()
        match = re.match(r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}.*)', clean_str)
        if match:
            data_str = match.group(1)
            ora_str = match.group(2)
        else:
            parts = clean_str.split(' ', 1)
            data_str = parts[0]
            ora_str = parts[1] if len(parts) > 1 else 'N/D'
        data_obj = datetime.strptime(data_str, '%Y-%m-%d')
        data_italiana = data_obj.strftime('%d/%m/%Y')
        return data_italiana, ora_str
    except:
        return datetime_str, 'N/D'

def estrai_dati_viaggio(driver, wait, mmsi):
    """Estrae i dati sia del viaggio attuale che di quello precedente."""
    print(f"  -> Tracciando MMSI partito: {mmsi}")
    url = f"{BASE_URL}/vessels/vessel-mmsi-{mmsi}-imo-0"
    dati_viaggi = {}
    try:
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.ID, "vpage-current-trip")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # ESTRAZIONE VIAGGIO ATTUALE
        sezione_viaggio_attuale = soup.find('div', id='vpage-current-trip')
        if sezione_viaggio_attuale:
            blocchi = sezione_viaggio_attuale.find_all('div', class_='myst-arrival-cont')
            if len(blocchi) >= 2:
                dati_viaggi['Porto Partenza Scraped'] = blocchi[0].find('h3').get_text(strip=True)
                partenza_raw = blocchi[0].find_all('span', class_='line')
                dati_viaggi['Data Partenza Completa'] = f"{partenza_raw[0].text.strip()} {partenza_raw[1].text.strip()}"
                
                dati_viaggi['Porto Arrivo'] = blocchi[1].find('h3').get_text(strip=True)
                arrivo_raw = blocchi[1].find_all('span', class_='line')
                dati_viaggi['Data Arrivo Completa'] = f"{arrivo_raw[0].text.strip()} {arrivo_raw[1].text.strip()}"
        
        # ESTRAZIONE VIAGGIO PRECEDENTE (OLD)
        sezione_storico = soup.find('div', id='ft-lasttrips')
        if sezione_storico and sezione_storico.find('tbody'):
            prima_riga = sezione_storico.find('tbody').find('tr')
            if prima_riga and prima_riga.find('td'):
                celle = prima_riga.find_all('td')
                if len(celle) >= 5:
                    dati_viaggi['Origin'] = celle[1].get_text(strip=True)
                    dati_viaggi['Departure'] = celle[2].get_text(strip=True)
                    dati_viaggi['Destination'] = celle[3].get_text(strip=True)
                    dati_viaggi['Arrival'] = celle[4].get_text(strip=True)
                    print(f"     -> Viaggio precedente trovato: da {dati_viaggi['Origin']} a {dati_viaggi['Destination']}")
        
        return dati_viaggi

    except Exception as e:
        print(f"     -> ATTENZIONE: Errore durante il tracciamento di MMSI {mmsi}. {e}")
        return None

def main():
    print("--- Avvio Script 2: Elaboratore ---")
    
    try:
        df_precedente = pd.read_csv(FILE_PRECEDENTE)
        df_attuale    = pd.read_csv(FILE_ATTUALE)

        # --- NEW: garantisco/uso RETRY_COUNT ed escludo oltre soglia ---
        if 'RETRY_COUNT' not in df_precedente.columns:
            df_precedente['RETRY_COUNT'] = 0
        df_precedente['RETRY_COUNT'] = pd.to_numeric(df_precedente['RETRY_COUNT'], errors='coerce').fillna(0).astype(int)

        df_prev_retry_ok = df_precedente[df_precedente['RETRY_COUNT'] < MAX_RETRY].copy()
        skipped = df_precedente.shape[0] - df_prev_retry_ok.shape[0]
        if skipped > 0:
            print(f"[RETRY] Skippati {skipped} MMSI con RETRY_COUNT >= {MAX_RETRY}")

        mappa_porti_precedenti = pd.Series(df_prev_retry_ok.PORTO.values, index=df_prev_retry_ok.MMSI.astype(str)).to_dict()
        set_precedente = set(df_prev_retry_ok['MMSI'].astype(str))
        set_attuale    = set(df_attuale['MMSI'].astype(str))
        navi_partite   = set_precedente.difference(set_attuale)
        # --- END NEW ---

        if not navi_partite:
            print("Nessuna nave è partita.")
        else:
            print(f"Trovate {len(navi_partite)} navi partite. Avvio browser per tracciamento...")

            # Avvio Chrome SOLO se serve davvero
            try:
                kill_zombie()
                driver, __prof = new_chrome_or_exit(headless=True)
                wait = WebDriverWait(driver, 15)
            except Exception as e:
                print(f"ERRORE: Impossibile avviare Chrome per il tracciamento. {e}")
                return

            dati_completi = []
            retry_entries = [] 
            for mmsi in navi_partite:
                porto_di_riferimento = mappa_porti_precedenti.get(mmsi, "SCONOSCIUTO")
                try:
                    dati_viaggio = estrai_dati_viaggio(driver, wait, mmsi)

                    # Se ho QUALSIASI dato utile, salvo (anche solo "viaggio precedente")
                    if dati_viaggio and (
                        'Porto Partenza Scraped' in dati_viaggio
                        or 'Origin' in dati_viaggio
                        or 'Departure' in dati_viaggio
                        or 'Destination' in dati_viaggio
                        or 'Arrival' in dati_viaggio
                    ):
                        dati_viaggio['MMSI'] = mmsi
                        dati_viaggio['Porto Partenza'] = porto_di_riferimento
                        dati_completi.append(dati_viaggio)
                    else:
                        # niente di utile: metto in retry per il prossimo ciclo
                        retry_entries.append((mmsi, porto_di_riferimento))

                except Exception as e:
                    print(f"     -> ATTENZIONE: Errore durante il tracciamento di MMSI {mmsi}. {e}")
                    retry_entries.append((mmsi, porto_di_riferimento))

                finally:
                    time.sleep(1)

            cleanup_profile(driver, __prof)

            # --- NEW: RE-QUEUE con incremento RETRY_COUNT in mmsi_attuali.csv ---
            try:
                if retry_entries:
                    print(f"[RETRY] Re-inserisco {len(retry_entries)} MMSI in mmsi_attuali.csv per il prossimo giro.")
                    try:
                        df_att = pd.read_csv(FILE_ATTUALE)
                    except FileNotFoundError:
                        df_att = pd.DataFrame(columns=["MMSI","DATA ESTRAZIONE","ORA ESTRAZIONE","PORTO","RETRY_COUNT"])

                    # normalizzo schema
                    for col in ["MMSI","DATA ESTRAZIONE","ORA ESTRAZIONE","PORTO","RETRY_COUNT"]:
                        if col not in df_att.columns:
                            df_att[col] = None

                    df_att["MMSI"] = df_att["MMSI"].astype(str).str.strip()
                    df_att["RETRY_COUNT"] = pd.to_numeric(df_att["RETRY_COUNT"], errors="coerce").fillna(0).astype(int)

                    oggi_data = datetime.now().strftime("%Y-%m-%d")
                    ora_now   = datetime.now().strftime("%H:%M:%S")

                    rows = []
                    for mmsi_retry, porto_ref in retry_entries:
                        rows.append({
                            "MMSI": str(mmsi_retry),
                            "DATA ESTRAZIONE": oggi_data,
                            "ORA ESTRAZIONE": ora_now,
                            "PORTO": porto_ref if isinstance(porto_ref, str) else "SCONOSCIUTO"
                        })
                    df_retry = pd.DataFrame(rows, columns=["MMSI","DATA ESTRAZIONE","ORA ESTRAZIONE","PORTO"])

                    # merge per incrementare contatore
                    df_merge = df_retry.merge(
                        df_att[["MMSI","RETRY_COUNT"]],
                        on="MMSI", how="left"
                    )
                    df_merge["RETRY_COUNT"] = df_merge["RETRY_COUNT"].fillna(0).astype(int) + 1

                    # upsert in df_att: sostituisco le vecchie righe dei medesimi MMSI
                    df_att = df_att[~df_att["MMSI"].isin(df_merge["MMSI"])]
                    df_upd = df_merge[["MMSI","DATA ESTRAZIONE","ORA ESTRAZIONE","PORTO","RETRY_COUNT"]]
                    df_att = pd.concat([df_att, df_upd], ignore_index=True)

                    # salvataggio atomico
                    tmp = FILE_ATTUALE + ".tmp"
                    df_att.to_csv(tmp, index=False)
                    os.replace(tmp, FILE_ATTUALE)

                    # log informativo: chi ha raggiunto soglia
                    over = df_upd[df_upd["RETRY_COUNT"] >= MAX_RETRY]["MMSI"].tolist()
                    if over:
                        print(f"[RETRY] Raggiunta soglia {MAX_RETRY} per {len(over)} MMSI: verranno ignorati dal prossimo ciclo.")
            except Exception as e:
                print(f"[RETRY] ATTENZIONE: non riesco a reinserire i falliti in mmsi_attuali.csv: {e}")
            # --- END NEW ---

            if dati_completi:
                PERCORSO_REPORT_MASTER = os.path.join(PATH_OUTPUT, 'Report_Navi_Tracciate_MASTER.xlsx')
                df_nuovi = pd.DataFrame(dati_completi)
                
                # --- GESTIONE DATI E COLONNE ---
                estrazione_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df_nuovi["Data Estrazione"] = estrazione_ts
                df_nuovi['Data Partenza'], df_nuovi['Ora Partenza'] = zip(*df_nuovi.get('Data Partenza Completa', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))
                df_nuovi['Data Arrivo'], df_nuovi['Ora Arrivo'] = zip(*df_nuovi.get('Data Arrivo Completa', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))
                
                df_nuovi['Porto Partenza (Old)'] = df_nuovi.get('Origin', 'NO DATA')
                df_nuovi['Data Partenza (Old)'], df_nuovi['Ora Partenza (Old)'] = zip(*df_nuovi.get('Departure', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))
                df_nuovi['Porto Arrivo (Old)'] = df_nuovi.get('Destination', 'NO DATA')
                df_nuovi['Data Arrivo (Old)'], df_nuovi['Ora Arrivo (Old)'] = zip(*df_nuovi.get('Arrival', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))

                try:
                    df_esistente = pd.read_excel(PERCORSO_REPORT_MASTER)
                    df_aggiornato = pd.concat([df_esistente, df_nuovi], ignore_index=True)
                except FileNotFoundError:
                    df_aggiornato = df_nuovi
                
                colonne_finali = [
                    'MMSI', 'Porto Partenza', 'Data Partenza', 'Ora Partenza', 
                    'Porto Arrivo', 'Data Arrivo', 'Ora Arrivo',
                    'Porto Partenza (Old)', 'Data Partenza (Old)', 'Ora Partenza (Old)',
                    'Porto Arrivo (Old)', 'Data Arrivo (Old)', 'Ora Arrivo (Old)','Data Estrazione'
                ]
                df_aggiornato = df_aggiornato.reindex(columns=colonne_finali)
                df_aggiornato.drop_duplicates(subset=['MMSI', 'Data Partenza', 'Ora Partenza'], keep='last', inplace=True)
                
                import os as __os
                percorso_senza_ext, estensione = __os.path.splitext(PERCORSO_REPORT_MASTER)
                __tmp = f"{percorso_senza_ext}_temp{estensione}"
                df_aggiornato.to_excel(__tmp, index=False)
                os.replace(__tmp, PERCORSO_REPORT_MASTER)
                safe_print(f"\n✅ Report aggiornato e salvato in: {PERCORSO_REPORT_MASTER}")

    except FileNotFoundError as e:
        print(f"ATTENZIONE: File non trovato durante il confronto. {e}")
    
    # --- BLOCCO STORICO + SWITCH (INVARIATO) ---
    try:
        # Archivio del precedente prima dello switch
        history_dir = os.path.join(PATH_FILE_TEMP, 'history')
        os.makedirs(history_dir, exist_ok=True)
        if os.path.exists(FILE_PRECEDENTE):
            stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            hist_path = os.path.join(history_dir, f"mmsi_precedenti_{stamp}.csv")
            try:
                shutil.copy2(FILE_PRECEDENTE, hist_path)
                print(f"Storico salvato: {hist_path}")
            except Exception as _e:
                print(f"ATTENZIONE: non riesco a salvare lo storico: {_e}")
            # Ora posso rimuovere il precedente prima del rename
            try:
                os.remove(FILE_PRECEDENTE)
            except OSError:
                pass
        if os.path.exists(FILE_ATTUALE):
            os.rename(FILE_ATTUALE, FILE_PRECEDENTE)
            print(f"File di stato aggiornati per la prossima esecuzione.")
    except OSError as e:
        print(f"ERRORE durante l'aggiornamento dei file di stato: {e}")
    
    print("--- FINE SCRIPT 2 ---")

if __name__ == "__main__":
    main()
