import sys
import os
from pathlib import Path

# Percorso base = cartella "Sviluppo"
BASE_DIR = Path(__file__).resolve().parent

# --- Screperino ---
# cartella script
SCREPERINO_DIR  = BASE_DIR / "Screperino" / "Screperino"
# cartella dati (input/output/log)
SCREPERINO_ROOT = BASE_DIR / "Screperino"

# --- MASE ---
# cartella script
MASE_DIR   = BASE_DIR / "MASE" / "script"
# cartelle di lavoro
MASE_ROOT   = BASE_DIR / "MASE"
MASE_INPUT  = MASE_ROOT / "Input"
MASE_OUTPUT = MASE_ROOT / "Output"
MASE_LOG    = MASE_ROOT / "log"

# sotto-cartelle MASE usate dagli script
MASE_INPUT_SCREP_PORTI = MASE_INPUT / "Screp" / "Porti"   # porti_screp.xlsx
MASE_TEMP              = MASE_INPUT / "File_Temp"         # mmsi_*.csv
MASE_OUTPUT_NAVI       = MASE_OUTPUT / "Navi_Estratte"    # Report_Navi_Tracciate_MASTER.xlsx
MASE_CHROME_PROFILE    = MASE_ROOT / "chrome-profile"     # user-data-dir Chrome

# Python interpreti (usa quello corrente)
PYTHON_A = sys.executable
PYTHON_B = sys.executable

# Path principali / stato
A_DIR   = SCREPERINO_DIR            # script Screperino
B_FILE  = MASE_DIR / "gestore2.py"  # entry MASE
LOG_DIR = SCREPERINO_ROOT / "Log"
LAST_B   = LOG_DIR / "last_b_run.txt"
LAST_EOD = LOG_DIR / "last_eod.txt"

# --- EOD / schedulazione ---
EOD_HOUR  = 23
EOD_MIN   = 30
B_EVERY_H = 4
A_MINUTE  = 5  # esegui A a :05

def ensure_dirs():
    """Crea le cartelle necessarie se mancano (Screperino + MASE)."""
    # Screperino
    for p in [
        SCREPERINO_ROOT / "File_Input" / "Bot",
        SCREPERINO_ROOT / "File_Input" / "MMSI",
        SCREPERINO_ROOT / "File_Input" / "Statici",
        SCREPERINO_ROOT / "File_Output" / "Estrazioni_Giornaliere",
        SCREPERINO_ROOT / "File_Output" / "Estrazioni_Giornaliere_Pulite",
        SCREPERINO_ROOT / "File_Output" / "Bot_Pulito",
        SCREPERINO_ROOT / "File_Output" / "Master",
        LOG_DIR,
    ]:
        os.makedirs(p, exist_ok=True)

    # MASE
    for p in [
        MASE_INPUT_SCREP_PORTI,
        MASE_TEMP,
        MASE_OUTPUT_NAVI,
        MASE_LOG,
        MASE_CHROME_PROFILE,
    ]:
        os.makedirs(p, exist_ok=True)
