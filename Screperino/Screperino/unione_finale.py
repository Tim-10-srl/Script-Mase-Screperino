import os
from pathlib import Path
from datetime import datetime, date
import logging
import pandas as pd
import bootstrap
from config import SCREPERINO_ROOT, LOG_DIR


def setup_logging(log_path: Path):
    log_path.mkdir(parents=True, exist_ok=True)
    log_file = log_path / "Log_Unione_Finale.txt"
    logger = logging.getLogger()
    if logger.hasHandlers():
        logger.handlers.clear()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filename=str(log_file),
        filemode="a",
        encoding="utf-8",
    )
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def unione_finale(data_da_processare: date | None = None):
    # --- PERCORSI DINAMICI: tutti sotto Screperino (root dati) ---
    base = Path(SCREPERINO_ROOT)
    path_giornaliera_pulita = base / "File_Output" / "Estrazioni_Giornaliere_Pulite"
    path_bot_pulito         = base / "File_Output" / "Bot_Pulito"
    path_statici            = base / "File_Input"  / "Statici"
    path_output_master      = base / "File_Output" / "Master"
    path_log_script         = Path(LOG_DIR)

    setup_logging(path_log_script)
    logging.info("================== AVVIO SCRIPT UNIONE FINALE ==================")

    if data_da_processare is None:
        data_da_processare = date.today()

    # --- 1) CARICAMENTO FILE ---
    try:
        nome_giornaliera = f"Giornaliera_Pulita_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
        nome_bot         = f"Bot_Pulito_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"

        fg = path_giornaliera_pulita / nome_giornaliera
        fb = path_bot_pulito / nome_bot

        # Check esplicito per input mancanti (log chiaro, niente stacktrace)
        missing = []
        if not fg.exists():
            missing.append(str(fg))
        if not fb.exists():
            missing.append(str(fb))
        if missing:
            logging.error("Mancano i seguenti input richiesti: " + " | ".join(missing))
            return

        df_giornaliera = pd.read_excel(fg)
        df_bot         = pd.read_excel(fb)
        df_specifiche  = pd.read_excel(path_statici / "MASTER_IHS FINALE.xlsx")
        df_decodifica  = pd.read_excel(path_statici / "DECODIFICA_FINALE.xlsx")
        logging.info("Tutti i file sorgente sono stati caricati.")
    except Exception as e:
        logging.error(f"ERRORE CRITICO nel caricamento file: {e}")
        return

    # --- 2) ARMONIZZAZIONE ---
    logging.info("Inizio armonizzazione...")
    for df in (df_giornaliera, df_bot, df_specifiche, df_decodifica):
        df.columns = [str(c).strip().lower() for c in df.columns]

    df_giornaliera.rename(columns={
        "origin": "porto partenza",
        "destination": "porto arrivo",
        "date departure": "data partenza",
    }, inplace=True)
    df_specifiche.rename(columns={"mmsi number": "mmsi"}, inplace=True)

    chiavi_da_convertire = {
        "df_giornaliera": ["mmsi", "porto partenza", "porto arrivo"],
        "df_specifiche":  ["mmsi"],
        "df_decodifica":  ["porto"],
        "df_bot":         ["porto partenza", "porto arrivo"],
    }
    loc = locals()
    for nome_df, keys in chiavi_da_convertire.items():
        df = loc[nome_df]
        for k in keys:
            if k in df.columns:
                df[k] = (
                    df[k].astype(str)
                         .str.replace(r"\.0$", "", regex=True)
                         .str.strip()
                         .str.lower()
                )

    df_giornaliera["data partenza"] = pd.to_datetime(
        df_giornaliera.get("data partenza"), errors="coerce"
    ).dt.date
    df_bot["data partenza"] = pd.to_datetime(
        df_bot.get("data partenza"), dayfirst=True, errors="coerce"
    ).dt.date

    # --- 3) UNIONE ---
    master_df = df_giornaliera
    logging.info("Inizio unione...")

    if "mmsi" in df_specifiche.columns and "mmsi" in master_df.columns:
        master_df = master_df.merge(df_specifiche, on="mmsi", how="left", suffixes=("", "_spec"))
        logging.info("Unite le specifiche della nave.")

    if {"porto", "nazione"} <= set(df_decodifica.columns):
        master_df = (
            master_df.merge(
                df_decodifica.rename(columns={"nazione": "nazione partenza"}),
                left_on="porto partenza", right_on="porto", how="left"
            ).drop(columns="porto", errors="ignore")
        )
        master_df = (
            master_df.merge(
                df_decodifica.rename(columns={"nazione": "nazione arrivo"}),
                left_on="porto arrivo", right_on="porto", how="left"
            ).drop(columns="porto", errors="ignore")
        )
        logging.info("Unite Nazione Partenza/Arrivo.")

    chiave_join = ["data partenza", "porto partenza", "porto arrivo"]
    if all(c in df_bot.columns for c in chiave_join):
        master_df = master_df.merge(df_bot, on=chiave_join, how="left")
        logging.info("Uniti dati dal Bot.")

    # --- 4) SALVATAGGIO ---
    path_output_master.mkdir(parents=True, exist_ok=True)
    nome_file_output = f"MASTER_{data_da_processare.strftime('%d-%m-%Y')}.xlsx"
    out_path = path_output_master / nome_file_output
    master_df.to_excel(out_path, index=False)
    logging.info(f"✅ UNIONE COMPLETATA! File salvato in: {out_path}")


if __name__ == "__main__":
    import sys
    raw = sys.argv[1].strip() if len(sys.argv) > 1 else ""
    d = None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            d = datetime.strptime(raw, fmt).date()
            break
        except Exception:
            pass
    unione_finale(d)
