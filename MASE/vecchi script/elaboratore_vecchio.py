# -*- coding: utf-8 -*-
# NOME FILE: elaboratore.py

import os
import re
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from zoneinfo import ZoneInfo          # stdlib (Py 3.9+)
from timezonefinder import TimezoneFinder

# --- CONFIGURAZIONE ---
PATH_FILE_TEMP = r'C:\Users\security\Documents\Codice\Python\MASE\Input\File_Temp'
FILE_PRECEDENTE = os.path.join(PATH_FILE_TEMP, 'mmsi_precedenti.csv')
FILE_ATTUALE = os.path.join(PATH_FILE_TEMP, 'mmsi_attuali.csv')
PATH_OUTPUT = r'C:\Users\security\Documents\Codice\Python\MASE\Output\Navi_Estratte'
BASE_URL = 'https://www.myshiptracking.com'

# File porti con lat/lon (usato anche da screp.py)
PORTS_FILE = r"C:\Users\security\Documents\Codice\Python\MASE\Input\ELENCO_PORTI.csv"
# colonne nel file porti:
COL_PORTO = "PORTO"   # oppure "Porto"
COL_LAT   = "LAT"     # oppure "Lat"
COL_LON   = "LON"     # oppure "Lon"

DEFAULT_TZ = 'Europe/Rome'  # fallback se un porto non è mappabile

# --- TIMEZONE HELPERS (lat/lon -> IANA tz) ---
_TF = TimezoneFinder()

def build_port_timezone_map():
    """
    Legge il file dei porti (CSV o Excel) con colonne PORTO, LAT, LON e crea una mappa {NOME_PORTO_UPPER: TZ_IANA}.
    """
    dfp = None
    try:
        dfp = pd.read_csv(PORTS_FILE)
    except Exception:
        try:
            dfp = pd.read_excel(PORTS_FILE)
        except Exception:
            print(f"ATTENZIONE: impossibile leggere PORTS_FILE: {PORTS_FILE}")
            return {}

    needed = {COL_PORTO, COL_LAT, COL_LON}
    if not needed.issubset(set(dfp.columns)):
        print(f"ATTENZIONE: file porti senza colonne richieste {needed}. Colonne trovate: {list(dfp.columns)}")
        return {}

    tzmap = {}
    for _, r in dfp.iterrows():
        try:
            name = str(r[COL_PORTO]).strip().upper()
            lat  = float(r[COL_LAT])
            lon  = float(r[COL_LON])
            tz   = _TF.timezone_at(lat=lat, lng=lon) or _TF.certain_timezone_at(lat=lat, lng=lon)
            if name and tz:
                tzmap[name] = tz
        except Exception:
            continue
    return tzmap

def tz_for_port(port_name: str, tzmap: dict, default_tz: str = DEFAULT_TZ) -> str:
    if not isinstance(port_name, str):
        return default_tz
    return tzmap.get(port_name.strip().upper(), default_tz)

def _clean_time_str(t: str) -> str:
    """Estrae HH:MM (o HH:MM:SS) da una stringa, altrimenti 'NO DATA'."""
    if not isinstance(t, str):
        return 'NO DATA'
    m = re.search(r'(\d{2}:\d{2}(?::\d{2})?)', t)
    return m.group(1) if m else 'NO DATA'

def to_utc_date_time(date_it: str, time_str: str, tz_name: str):
    """
    Converte (dd/mm/YYYY, HH:MM[:SS]) dal fuso locale tz_name a UTC.
    Ritorna ('dd/mm/YYYY','HH:MM') o ('NO DATA','NO DATA').
    """
    try:
        if not date_it or not time_str or date_it == 'NO DATA' or time_str == 'NO DATA':
            return 'NO DATA', 'NO DATA'
        time_str = _clean_time_str(time_str)
        if time_str == 'NO DATA':
            return 'NO DATA', 'NO DATA'
        dt_naive = None
        for fmt in ('%d/%m/%Y %H:%M:%S', '%d/%m/%Y %H:%M'):
            try:
                dt_naive = datetime.strptime(f'{date_it} {time_str}', fmt)
                break
            except ValueError:
                continue
        if dt_naive is None:
            return 'NO DATA', 'NO DATA'
        dt_local = dt_naive.replace(tzinfo=ZoneInfo(tz_name or DEFAULT_TZ))
        dt_utc   = dt_local.astimezone(ZoneInfo('UTC'))
        return dt_utc.strftime('%d/%m/%Y'), dt_utc.strftime('%H:%M')
    except Exception:
        return 'NO DATA', 'NO DATA'

# --- PARSING HELPERS ---
def separa_data_ora_e_formatta(datetime_str):
    """
    Separa data/ora e formatta la data in gg/mm/aaaa.
    Gestisce 'YYYY-MM-DD HH:MM' e 'YYYY-MM-DDHH:MM'.
    Ritorna sempre stringhe; in caso di problemi 'NO DATA'.
    """
    if pd.isna(datetime_str) or not isinstance(datetime_str, str):
        return 'NO DATA', 'NO DATA'
    try:
        clean_str = re.sub('<[^<]+?>', '', datetime_str).strip()
        m = re.match(r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}.*)', clean_str)
        if m:
            data_str, ora_str = m.group(1), m.group(2)
        else:
            parts = clean_str.split(' ', 1)
            data_str = parts[0]
            ora_str = parts[1] if len(parts) > 1 else 'NO DATA'
        data_italiana = datetime.strptime(data_str, '%Y-%m-%d').strftime('%d/%m/%Y')
        return data_italiana, ora_str
    except:
        return 'NO DATA', 'NO DATA'

def estrai_dati_viaggio(driver, wait, mmsi):
    """Estrae i dati sia del viaggio attuale che di quello precedente."""
    print(f"  -> Tracciando MMSI partito: {mmsi}")
    url = f"{BASE_URL}/vessels/vessel-mmsi-{mmsi}-imo-0"
    dati_viaggi = {}
    try:
        driver.get(url)
        wait.until(EC.presence_of_element_located((By.ID, "vpage-current-trip")))
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        # VIAGGIO ATTUALE
        sezione_viaggio_attuale = soup.find('div', id='vpage-current-trip')
        if sezione_viaggio_attuale:
            blocchi = sezione_viaggio_attuale.find_all('div', class_='myst-arrival-cont')
            if len(blocchi) >= 2:
                dati_viaggi['Porto Partenza Scraped'] = blocchi[0].find('h3').get_text(strip=True)
                partenza_raw = blocchi[0].find_all('span', class_='line')
                if len(partenza_raw) >= 2:
                    dati_viaggi['Data Partenza Completa'] = f"{partenza_raw[0].text.strip()} {partenza_raw[1].text.strip()}"

                dati_viaggi['Porto Arrivo'] = blocchi[1].find('h3').get_text(strip=True)
                arrivo_raw = blocchi[1].find_all('span', class_='line')
                if len(arrivo_raw) >= 2:
                    dati_viaggi['Data Arrivo Completa'] = f"{arrivo_raw[0].text.strip()} {arrivo_raw[1].text.strip()}"

        # VIAGGIO PRECEDENTE (OLD)
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
        df_attuale = pd.read_csv(FILE_ATTUALE)

        # Mapping PORTO robusto (nome colonna può cambiare)
        if 'PORTO' in df_precedente.columns:
            col_porto = 'PORTO'
        elif 'Porto' in df_precedente.columns:
            col_porto = 'Porto'
        else:
            col_porto = None

        if col_porto:
            mappa_porti_precedenti = pd.Series(
                df_precedente[col_porto].values,
                index=df_precedente['MMSI'].astype(str)
            ).to_dict()
        else:
            mappa_porti_precedenti = {}

        set_precedente = set(df_precedente['MMSI'].astype(str))
        set_attuale = set(df_attuale['MMSI'].astype(str))
        navi_partite = set_precedente.difference(set_attuale)

        if not navi_partite:
            print("Nessuna nave è partita.")
        else:
            print(f"Trovate {len(navi_partite)} navi partite. Avvio browser per tracciamento...")

            options = Options()
            try:
                options.add_argument(r"user-data-dir=C:\Users\security\chrome-test-profile")
                driver = webdriver.Chrome(options=options)
                wait = WebDriverWait(driver, 15)
            except Exception as e:
                print(f"ERRORE: Impossibile avviare Chrome per il tracciamento. {e}")
                return

            dati_completi = []
            try:
                for mmsi in navi_partite:
                    porto_di_riferimento = mappa_porti_precedenti.get(mmsi, "SCONOSCIUTO")
                    dati_viaggio = estrai_dati_viaggio(driver, wait, mmsi)

                    if dati_viaggio and 'Porto Partenza Scraped' in dati_viaggio:
                        if porto_di_riferimento and porto_di_riferimento.lower() in dati_viaggio['Porto Partenza Scraped'].lower():
                            dati_viaggio['MMSI'] = mmsi
                            dati_viaggio['Porto Partenza'] = porto_di_riferimento
                            dati_completi.append(dati_viaggio)
                    time.sleep(1)
            finally:
                driver.quit()

            if dati_completi:
                PERCORSO_REPORT_MASTER = os.path.join(PATH_OUTPUT, 'Report_Navi_Tracciate_MASTER.xlsx')
                df_nuovi = pd.DataFrame(dati_completi)

                # --- GESTIONE DATI E COLONNE (LOCAL TIME DAL SITO) ---
                df_nuovi['Data Partenza'], df_nuovi['Ora Partenza'] = zip(*df_nuovi.get('Data Partenza Completa', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))
                df_nuovi['Data Arrivo'], df_nuovi['Ora Arrivo'] = zip(*df_nuovi.get('Data Arrivo Completa', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))

                df_nuovi['Porto Partenza (Old)'] = df_nuovi.get('Origin', 'NO DATA')
                df_nuovi['Data Partenza (Old)'], df_nuovi['Ora Partenza (Old)'] = zip(*df_nuovi.get('Departure', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))
                df_nuovi['Porto Arrivo (Old)'] = df_nuovi.get('Destination', 'NO DATA')
                df_nuovi['Data Arrivo (Old)'], df_nuovi['Ora Arrivo (Old)'] = zip(*df_nuovi.get('Arrival', pd.Series(dtype='str')).fillna('NO DATA').apply(separa_data_ora_e_formatta))

                # --- COSTRUISCI MAPPA PORTO -> TIMEZONE DAI LAT/LON ---
                tzmap = build_port_timezone_map()

                # --- CONVERSIONE A UTC ---
                # Partenza -> usa fuso del Porto Partenza
                part_utc = df_nuovi.apply(
                    lambda r: to_utc_date_time(
                        r.get('Data Partenza', 'NO DATA'),
                        r.get('Ora Partenza', 'NO DATA'),
                        tz_for_port(r.get('Porto Partenza'), tzmap)
                    ),
                    axis=1
                )
                df_nuovi['Data Partenza UTC'], df_nuovi['Ora Partenza UTC'] = zip(*part_utc)

                # Arrivo -> usa fuso del Porto Arrivo
                arr_utc = df_nuovi.apply(
                    lambda r: to_utc_date_time(
                        r.get('Data Arrivo', 'NO DATA'),
                        r.get('Ora Arrivo', 'NO DATA'),
                        tz_for_port(r.get('Porto Arrivo'), tzmap)
                    ),
                    axis=1
                )
                df_nuovi['Data Arrivo UTC'], df_nuovi['Ora Arrivo UTC'] = zip(*arr_utc)

                # (Opzionale ma utile per audit) salva anche i fusi usati
                df_nuovi['Fuso Partenza'] = df_nuovi['Porto Partenza'].apply(lambda p: tz_for_port(p, tzmap))
                df_nuovi['Fuso Arrivo']   = df_nuovi['Porto Arrivo'].apply(lambda p: tz_for_port(p, tzmap))

                # --- MERGE CON MASTER E SALVATAGGIO ---
                try:
                    df_esistente = pd.read_excel(PERCORSO_REPORT_MASTER)
                    df_aggiornato = pd.concat([df_esistente, df_nuovi], ignore_index=True)
                except FileNotFoundError:
                    df_aggiornato = df_nuovi

                colonne_finali = [
                    'MMSI',
                    'Porto Partenza', 'Fuso Partenza', 'Data Partenza', 'Ora Partenza',      # LOCAL
                    'Data Partenza UTC', 'Ora Partenza UTC',                                  # UTC
                    'Porto Arrivo',   'Fuso Arrivo',   'Data Arrivo',   'Ora Arrivo',        # LOCAL
                    'Data Arrivo UTC','Ora Arrivo UTC',                                       # UTC
                    'Porto Partenza (Old)', 'Data Partenza (Old)', 'Ora Partenza (Old)',
                    'Porto Arrivo (Old)',   'Data Arrivo (Old)',   'Ora Arrivo (Old)'
                ]
                df_aggiornato = df_aggiornato.reindex(columns=colonne_finali)

                # Dedup sugli identificativi robusti in UTC
                df_aggiornato.drop_duplicates(
                    subset=['MMSI', 'Porto Partenza', 'Porto Arrivo', 'Data Partenza UTC', 'Ora Partenza UTC'],
                    keep='last',
                    inplace=True
                )

                if not df_aggiornato.empty:
                    os.makedirs(PATH_OUTPUT, exist_ok=True)
                    df_aggiornato.to_excel(PERCORSO_REPORT_MASTER, index=False)
                    print(f"\n✅ Report aggiornato e salvato in: {PERCORSO_REPORT_MASTER}")
                else:
                    print("Nessun record valido da salvare nel report.")

    except FileNotFoundError as e:
        print(f"ATTENZIONE: File non trovato durante il confronto. {e}")

    # --- BLOCCO FINALE: archiviazione e promozione stato ---
    try:
        if os.path.exists(FILE_ATTUALE):
            # 1) L'attuale ESISTE: archivia il precedente e promuovi l'attuale
            if os.path.exists(FILE_PRECEDENTE):
                ts = datetime.now().strftime('%Y%m%d_%H%M%S_%f')  # microsecondi per univocità
                storico_dir = os.path.join(PATH_FILE_TEMP, "storico")
                os.makedirs(storico_dir, exist_ok=True)
                nome_storico = os.path.join(storico_dir, f"storico_precedenti_{ts}.csv")
                os.replace(FILE_PRECEDENTE, nome_storico)
                print(f"Archivio creato: {nome_storico}")

            os.replace(FILE_ATTUALE, FILE_PRECEDENTE)
            print("File di stato aggiornati per la prossima esecuzione (storico salvato).")
        else:
            # 2) L'attuale NON c'è: NON toccare il precedente, altrimenti perdi lo stato
            print("ATTENZIONE: FILE_ATTUALE mancante; stato NON aggiornato e precedente NON archiviato.")
    except OSError as e:
        print(f"ERRORE durante l'archiviazione/aggiornamento dei file di stato: {e}")

    print("--- FINE SCRIPT 2 ---")

if __name__ == "__main__":
    main()
