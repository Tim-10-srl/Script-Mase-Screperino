import pandas as pd
import os
from datetime import datetime, date
import logging

def setup_logging(log_path):
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, 'Log_Unione_Finale.txt')
    logger = logging.getLogger()
    if logger.hasHandlers(): logger.handlers.clear()
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file, filemode='a', encoding='utf-8')
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

def unione_finale(data_da_processare=None):
    # --- CONFIGURAZIONE DEI PERCORSI ---
    path_giornaliera_pulita = r"C:\Users\security\Documents\Codice\Python\Screperino\File_Output\Estrazioni_Giornaliere_Pulite"
    path_bot_pulito = r"C:\Users\security\Documents\Codice\Python\Screperino\File_Output\Bot_Pulito"
    path_statici = r"C:\Users\security\Documents\Codice\Python\Screperino\File_Input\Statici"
    path_output_master = r"C:\Users\security\Documents\Codice\Python\Screperino\File_Output\Master"
    path_log_script = r"C:\Users\security\Documents\Codice\Python\Screperino\Log"

    setup_logging(path_log_script)
    logging.info("================== AVVIO SCRIPT UNIONE FINALE ==================")

    if data_da_processare is None:
        data_da_processare = date.today()
    
    # --- 1. CARICAMENTO FILE ---
    # ... (caricamento file come prima) ...
    try:
        nome_giornaliera = f"Giornaliera_Pulita_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
        nome_bot = f"Bot_Pulito_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
        df_giornaliera = pd.read_excel(os.path.join(path_giornaliera_pulita, nome_giornaliera))
        df_bot = pd.read_excel(os.path.join(path_bot_pulito, nome_bot))
        df_specifiche = pd.read_excel(os.path.join(path_statici, "MASTER_IHS FINALE.xlsx"))
        df_decodifica = pd.read_excel(os.path.join(path_statici, "DECODIFICA_FINALE.xlsx"))
        logging.info("Tutti i file sorgente sono stati caricati.")
    except Exception as e:
        logging.error(f"ERRORE CRITICO nel caricamento file: {e}"); return

    # --- 2. ARMONIZZAZIONE COLONNE E TIPI DI DATO ---
    logging.info("Inizio armonizzazione...")
    df_giornaliera.columns = [str(col).strip().lower() for col in df_giornaliera.columns]
    df_bot.columns = [str(col).strip().lower() for col in df_bot.columns]
    df_specifiche.columns = [str(col).strip().lower() for col in df_specifiche.columns]
    df_decodifica.columns = [str(col).strip().lower() for col in df_decodifica.columns]
    
    df_giornaliera.rename(columns={'origin': 'porto partenza', 'destination': 'porto arrivo', 'date departure': 'data partenza'}, inplace=True)
    df_specifiche.rename(columns={'mmsi number': 'mmsi'}, inplace=True)

    # --- MODIFICA DEFINITIVA: Conversione di tutte le chiavi a TESTO (string) ---
    chiavi_da_convertire = {
        'df_giornaliera': ['mmsi', 'porto partenza', 'porto arrivo'],
        'df_specifiche': ['mmsi'],
        'df_decodifica': ['porto'],
        'df_bot': ['porto partenza', 'porto arrivo']
    }
    
    # Converte le chiavi in testo per garantire la corrispondenza
    for df_name, keys in chiavi_da_convertire.items():
        df = locals()[df_name]
        for key in keys:
            if key in df.columns:
                # Rimuove ".0" dai numeri e poi converte in testo
                df[key] = df[key].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().str.lower()
    
    # Standardizza le date
    df_giornaliera['data partenza'] = pd.to_datetime(df_giornaliera['data partenza'], errors='coerce').dt.date
    df_bot['data partenza'] = pd.to_datetime(df_bot['data partenza'], dayfirst=True, errors='coerce').dt.date

    # --- 3. UNIONE (MERGE) ---
    master_df = df_giornaliera
    logging.info("Inizio unione...")

    # Unione con specifiche nave tramite MMSI
    if 'mmsi' in df_specifiche.columns and 'mmsi' in master_df.columns:
        master_df = pd.merge(master_df, df_specifiche, on='mmsi', how='left', suffixes=('', '_spec'))
        logging.info("Unite le specifiche della nave.")
    
    # Unione Nazioni
    if 'porto' in df_decodifica.columns and 'nazione' in df_decodifica.columns:
        master_df = pd.merge(master_df, df_decodifica.rename(columns={'nazione': 'nazione partenza'}), left_on='porto partenza', right_on='porto', how='left').drop(columns='porto', errors='ignore')
        master_df = pd.merge(master_df, df_decodifica.rename(columns={'nazione': 'nazione arrivo'}), left_on='porto arrivo', right_on='porto', how='left').drop(columns='porto', errors='ignore')
        logging.info("Unita Nazione Partenza e Arrivo.")
    
    # Unione Dati Bot
    chiave_join = ['data partenza', 'porto partenza', 'porto arrivo']
    if all(col in df_bot.columns for col in chiave_join):
        master_df = pd.merge(master_df, df_bot, on=chiave_join, how='left')
        logging.info("Uniti dati dal Bot.")

    # --- 4. SALVATAGGIO ---
    os.makedirs(path_output_master, exist_ok=True)
    nome_file_output = f"MASTER_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
    percorso_completo_output = os.path.join(path_output_master, nome_file_output)
    master_df.to_excel(percorso_completo_output, index=False)
    logging.info(f"✅ UNIONE COMPLETATA! File salvato in: {percorso_completo_output}")

if __name__ == "__main__":
    unione_finale()