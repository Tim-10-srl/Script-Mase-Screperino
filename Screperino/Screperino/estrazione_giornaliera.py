# --- estrazione_giornaliera.py ---
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
import time
import logging
import os
import bootstrap
from config import SCREPERINO_ROOT  # aggiungi in cima all'import dal config




# ... (le funzioni setup_logging e separa_data_ora rimangono invariate) ...

def setup_logging(log_file_path):
    try:
        log_dir = os.path.dirname(log_file_path)
        os.makedirs(log_dir, exist_ok=True)
        logger = logging.getLogger()
        if logger.hasHandlers():
            logger.handlers.clear()
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=log_file_path,
            filemode='a',
            encoding='utf-8'
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logging.getLogger().addHandler(console_handler)
    except Exception as e:
        print(f"ERRORE CRITICO: Impossibile configurare il logging. Dettagli: {e}")
        exit()

def separa_data_ora(datetime_str):
    testo = datetime_str.strip()
    if len(testo) > 10 and testo[4] == '-' and testo[7] == '-':
        data, ora = testo[:10], testo[10:]
        if ':' in ora:
            return data, ora
    return testo, 'N/A'

def estrai_dati_nave(mmsi):
    url = f"https://www.myshiptracking.com/vessels/vessel-mmsi-{mmsi}-imo-0"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    logging.info(f"Richiesta dati per MMSI: {mmsi}")
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"ERRORE DI RETE per MMSI {mmsi}. Dettagli: {e}")
        return None
    
    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Estrae sia IMO che MMSI dalla tabella dei dettagli
        imo_trovato = 'N/A'
        imo_header_tag = soup.find('b', string='IMO')
        if imo_header_tag and imo_header_tag.find_parent('td') and imo_header_tag.find_parent('td').find_next_sibling('td'):
            imo_trovato = imo_header_tag.find_parent('td').find_next_sibling('td').get_text(strip=True)

        dati_base = {'MMSI': mmsi, 'IMO': imo_trovato}
        
        sezione_viaggi = soup.find('div', id='ft-lasttrips')
        tabella = sezione_viaggi.find('table', class_='myst-table') if sezione_viaggi else None
        if not tabella or not tabella.find('tbody'):
            logging.warning(f"AVVISO: Tabella viaggi non trovata per MMSI {mmsi}. Salvo solo MMSI e IMO.")
            return dati_base

        righe = tabella.find('tbody').find_all('tr')
        if righe:
            prima_riga = righe[0]
            celle = prima_riga.find_all('td')
            if len(celle) > 5:
                date_departure, time_departure = separa_data_ora(celle[2].get_text(strip=True))
                date_arrival, time_arrival = separa_data_ora(celle[4].get_text(strip=True))
                duration_cell = prima_riga.find('td', class_='table-more-td')
                duration = duration_cell.get('data-dur', 'N/A').strip() if duration_cell else 'N/A'
                viaggio_info = {
                    'Origin': celle[1].get_text(strip=True),
                    'Date Departure': date_departure,
                    'Time Departure': time_departure,
                    'Destination': celle[3].get_text(strip=True),
                    'Date Arrival': date_arrival,
                    'Time Arrival': time_arrival,
                    'Duration': duration,
                    'Distance': celle[5].get_text(strip=True)
                }
                dati_base.update(viaggio_info)
                logging.info(f"Trovato ultimo viaggio per MMSI {mmsi}.")
        return dati_base
    except Exception as e:
        logging.error(f"ERRORE DI PARSING per MMSI {mmsi}. Dettagli: {e}")
        return None

def main():

    path_input = os.path.join(SCREPERINO_ROOT, "File_Input", "MMSI")
    path_output_base = os.path.join(SCREPERINO_ROOT, "File_Output", "Estrazioni_Giornaliere")
    path_log = os.path.join(SCREPERINO_ROOT, "Log", "Log_Estrazione.log")

    setup_logging(path_log)
    logging.info("================== AVVIO SCRIPT DI ESTRAZIONE (MMSI+IMO) ==================")

    file_input = os.path.join(path_input, 'MMSI.xlsx')
    try:
        df_input = pd.read_excel(file_input)
        if 'MMSI' not in df_input.columns:
            logging.error(f"ERRORE CRITICO: Colonna 'MMSI' non trovata in '{file_input}'.")
            return
        lista_mmsi = df_input['MMSI'].dropna().astype(int).tolist()
    except Exception as e:
        logging.error(f"ERRORE CRITICO: Impossibile leggere il file di input. Dettagli: {e}")
        return
    
    dati_totali = [dati_nave for mmsi in lista_mmsi if (dati_nave := estrai_dati_nave(str(mmsi))) is not None]
    time.sleep(1)

    if dati_totali:
        try:
            cartella_giornaliera = f"Estrazioni_{datetime.now().strftime('%d_%m_%Y')}"
            path_output_giornaliero = os.path.join(path_output_base, cartella_giornaliera)
            os.makedirs(path_output_giornaliero, exist_ok=True)
            timestamp = datetime.now().strftime("%d_%m_%Y-%H_%M")
            nome_file_output = f'Estrazione_Giornaliera_{timestamp}.xlsx'
            percorso_completo_output = os.path.join(path_output_giornaliero, nome_file_output)
            df_output = pd.DataFrame(dati_totali)
            # Assicura che IMO sia presente e all'inizio
            colonne_ordinate = ['MMSI', 'IMO', 'Origin', 'Date Departure', 'Time Departure', 'Destination', 'Date Arrival', 'Time Arrival', 'Duration', 'Distance']
            df_output = df_output.reindex(columns=colonne_ordinate)
            df_output.to_excel(percorso_completo_output, index=False)
            logging.info(f"✅ Estrazione completata! Dati salvati in: {percorso_completo_output}")
        except Exception as e:
            logging.error(f"ERRORE CRITICO: Impossibile salvare il file di output. Dettagli: {e}")
    else:
        logging.warning("❌ Estrazione completata, ma nessun dato è stato raccolto.")

if __name__ == "__main__":
    main()
