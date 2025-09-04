import pandas as pd
import os
from datetime import datetime, date, timedelta
import logging
import bootstrap

# importa i path centralizzati
from config import SCREPERINO_ROOT

def setup_logging(log_path):
    """Configura il logging per scrivere su un percorso specifico."""
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, 'Log_Pulizia_Bot.txt')
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='a',
        encoding='utf-8'
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logging.getLogger().addHandler(console_handler)

def pulisci_bot(data_da_processare=None):
    """
    Pulisce i file del bot per un giorno specifico, scansionando tutti i file ogni volta.
    """
    path_input_bot  = os.path.join(SCREPERINO_ROOT, "File_Input", "Bot")
    path_output_bot = os.path.join(SCREPERINO_ROOT, "File_Output", "Bot_Pulito")
    path_log_script = os.path.join(SCREPERINO_ROOT, "Log")


    setup_logging(path_log_script)
    logging.info("================== AVVIO SCRIPT PULIZIA BOT ==================")

    if data_da_processare is None:
        data_da_processare = date.today()

    if not os.path.isdir(path_input_bot):
        logging.warning(f"Cartella bot inesistente: {path_input_bot}. Skip.")
        return

    
    file_da_processare = [
        f for f in os.listdir(path_input_bot)
        if f.endswith('.xlsx') and not f.startswith('~$')
    ]
    
    if not file_da_processare:
        logging.warning("Nessun file .xlsx trovato nella cartella del bot.")
        return

    logging.info(f"Trovati {len(file_da_processare)} file da analizzare per la data {data_da_processare.strftime('%d-%m-%Y')}.")

    dati_trovati = []
    for filename in file_da_processare:
        percorso_file = os.path.join(path_input_bot, filename)
        try:
            df_temp = pd.read_excel(percorso_file, sheet_name='TRATTE', header=0)
            
            df_temp.columns = [str(col).strip().lower() for col in df_temp.columns]
            
            required_cols = ['data partenza', 'orario partenza']
            if not all(col in df_temp.columns for col in required_cols):
                logging.warning(f"AVVISO: Colonne richieste ('data partenza', 'orario partenza') non trovate nel file {filename}. File saltato.")
                continue

            df_temp['DataOraCompleta'] = pd.to_datetime(
                df_temp['data partenza'].astype(str) + ' ' + df_temp['orario partenza'].astype(str),
                dayfirst=True,
                errors='coerce'
            )
            
            df_filtrato = df_temp[df_temp['DataOraCompleta'].dt.date == data_da_processare].copy()
            
            if not df_filtrato.empty:
                logging.info(f"Trovate {len(df_filtrato)} righe nel file {filename}.")
                df_filtrato = df_filtrato.drop(columns=['DataOraCompleta'], errors='ignore')
                dati_trovati.append(df_filtrato)

        except Exception as e:
            logging.error(f"Impossibile processare il file {filename}. Errore: {e}")

    if not dati_trovati:
        logging.warning(f"Nessun dato trovato per la data {data_da_processare.strftime('%d-%m-%Y')} in nessun file.")
        return
        
    df_totale = pd.concat(dati_trovati, ignore_index=True)
    logging.info(f"Righe totali trovate prima della pulizia: {len(df_totale)}")

    df_totale.dropna(how='all', inplace=True)
    df_totale.drop_duplicates(inplace=True)
    logging.info(f"Numero finale di righe uniche: {len(df_totale)}")

    if not df_totale.empty:
        os.makedirs(path_output_bot, exist_ok=True)
        nome_file_output = f"Bot_Pulito_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
        percorso_completo_output = os.path.join(path_output_bot, nome_file_output)
        
        colonne_da_salvare = [
            col for col in [
                'data partenza', 'nave', 'porto partenza', 'orario partenza',
                'porto arrivo', 'orario arrivo', 'durata viaggio',
                'operatore', 'prezzo', 'fonte', 'note'
            ] if col in df_totale.columns
        ]
        df_totale = df_totale[colonne_da_salvare]

        df_totale.to_excel(percorso_completo_output, index=False)
        logging.info(f"✅ Dati del bot puliti e salvati in: {percorso_completo_output}")

def _parse_cli_date():
    import sys
    raw = sys.argv[1].strip() if len(sys.argv) >= 2 else ""
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            pass
    return None  # niente data -> ci pensa il default nel corpo

if __name__ == "__main__":
    d = _parse_cli_date()
    pulisci_bot(d or date.today())
    logging.info("================== FINE SCRIPT PULIZIA BOT ===================\n")
