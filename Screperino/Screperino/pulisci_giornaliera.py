# -*- coding: utf-8 -*-
"""
pulisci_giornaliere.py
Consolida e pulisce le estrazioni giornaliere .xlsx di una data.
- Se non passi una data: usa OGGI.
- Se passi una data CLI in formato ISO (YYYY-MM-DD), usa quella.

Exit code:
  0 -> Successo (o nessun lavoro da fare: nessuna cartella/file)
  2 -> Errore reale (tutti i file falliscono la lettura, concat fallita, ecc.)
"""

import os
import sys
import logging
from datetime import datetime, date, timedelta
import bootstrap

import pandas as pd

# importa i path centralizzati
from config import SCREPERINO_ROOT

# =========================
# Config percorsi
# =========================
PATH_INPUT_BASE    = os.path.join(SCREPERINO_ROOT, "File_Output", "Estrazioni_Giornaliere")
PATH_OUTPUT_PULITE = os.path.join(SCREPERINO_ROOT, "File_Output", "Estrazioni_Giornaliere_Pulite")
PATH_LOG           = os.path.join(SCREPERINO_ROOT, "Log")
LOG_FILE           = os.path.join(PATH_LOG, "Log_Pulizia.txt")


# =========================
# Logging
# =========================
def setup_logging():
    os.makedirs(PATH_LOG, exist_ok=True)

    # Ripulisce eventuali handler duplicati sul root logger
    root = logging.getLogger()
    if root.hasHandlers():
        root.handlers.clear()

    root.setLevel(logging.INFO)

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(fmt)
    root.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    root.addHandler(ch)


# =========================
# Core
# =========================
def pulisci_giornaliere(data_da_processare: date | None = None) -> int:
    """
    Consolida e pulisce le estrazioni .xlsx per un giorno specifico.

    Ritorna:
        0  = successo (anche se non c'è nulla da processare)
        2  = errore reale (nessun file leggibile, concat fallita, ecc.)
    """
    setup_logging()
    logging.info("================== AVVIO SCRIPT DI PULIZIA ==================")

    # Se non viene fornita una data, si usa quella di oggi
    if data_da_processare is None:
        data_da_processare = date.today()

    # Cartella attesa: Estrazioni_DD_MM_YYYY
    nome_cartella = f"Estrazioni_{data_da_processare.strftime('%d_%m_%Y')}"
    path_input_specifico = os.path.join(PATH_INPUT_BASE, nome_cartella)

    if not os.path.isdir(path_input_specifico):
        logging.warning(
            f"Nessuna cartella di input per la data {data_da_processare.strftime('%d-%m-%Y')}: "
            f"{path_input_specifico}. Esco senza errore."
        )
        logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
        return 0

    logging.info(f"Cartella input: {path_input_specifico}")

    # Prende solo .xlsx validi, evitando i temporanei di Excel (~$...)
    try:
        entries = os.listdir(path_input_specifico)
    except Exception as e:
        logging.error(f"Impossibile leggere il contenuto della cartella: {e}")
        logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
        return 2

    lista_file = [
        os.path.join(path_input_specifico, f)
        for f in entries
        if f.lower().endswith(".xlsx") and not f.startswith("~$")
    ]

    if not lista_file:
        logging.warning(
            f"Nessun file .xlsx valido nella cartella per la data "
            f"{data_da_processare.strftime('%d-%m-%Y')}. Esco senza errore."
        )
        logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
        return 0

    # Lettura file con engine esplicito
    df_list = []
    for file in lista_file:
        try:
            df_temp = pd.read_excel(file, engine="openpyxl")
            df_list.append(df_temp)
            logging.info(f"Letto: {os.path.basename(file)} (righe: {len(df_temp)})")
        except Exception as e:
            logging.error(f"Impossibile leggere il file '{file}'. Errore: {e}")

    if not df_list:
        logging.error(
            "Tutti i file hanno fallito la lettura: nessun DataFrame disponibile. "
            "Esco con codice errore."
        )
        logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
        return 2

    # Concatenazione sicura
    try:
        df_totale = pd.concat(df_list, ignore_index=True)
    except Exception as e:
        logging.exception(f"Concat fallita: {e}")
        logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
        return 2

    logging.info(f"Uniti {len(df_list)} file. Righe totali prima della pulizia: {len(df_totale)}")

    # Pulizia
    righe_prima = len(df_totale)
    df_totale.dropna(how="all", inplace=True)
    logging.info(f"Rimosse {righe_prima - len(df_totale)} righe completamente vuote.")

    righe_prima = len(df_totale)
    df_totale.drop_duplicates(inplace=True)
    logging.info(f"Rimosse {righe_prima - len(df_totale)} righe duplicate.")
    logging.info(f"Numero finale di righe uniche: {len(df_totale)}")

    # Salvataggio
    try:
        os.makedirs(PATH_OUTPUT_PULITE, exist_ok=True)
        nome_file_output = f"Giornaliera_Pulita_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
        percorso_output = os.path.join(PATH_OUTPUT_PULITE, nome_file_output)
        df_totale.to_excel(percorso_output, index=False)
        logging.info(f"✅ Pulizia completata! Dati salvati in: {percorso_output}")
    except Exception as e:
        logging.error(f"ERRORE salvataggio file pulito: {e}")
        logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
        return 2

    logging.info("================== FINE SCRIPT DI PULIZIA ===================\n")
    return 0


# =========================
# CLI
# =========================
def _parse_cli_date() -> date | None:
    """Parsa una data opzionale da CLI in formato ISO (YYYY-MM-DD)."""
    if len(sys.argv) >= 2:
        raw = sys.argv[1].strip()
        try:
            return datetime.fromisoformat(raw).date()
        except Exception:
            # supporto rapido per formati comuni DD-MM-YYYY / DD/MM/YYYY
            for fmt in ("%d-%m-%Y", "%d/%m/%Y"):
                try:
                    return datetime.strptime(raw, fmt).date()
                except Exception:
                    pass
            logging.warning(f"Data CLI non valida: '{raw}'. Uso la data odierna.")
    return None


if __name__ == "__main__":
    d = _parse_cli_date()
    rc = pulisci_giornaliere(d)
    sys.exit(rc if isinstance(rc, int) else 0)
