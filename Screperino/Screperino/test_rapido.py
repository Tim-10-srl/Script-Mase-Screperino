import time
import os
import subprocess
import logging
from datetime import datetime

# --- CONFIGURAZIONE ---
PATH_PROGETTO = r"C:\Users\security\Documents\Codice\Python\Screperino\Screperino"
PYTHON_EXE = r"C:\Users\security\AppData\Local\anaconda3\python.exe"
PATH_LOG = r"C:\Users\security\Documents\Codice\Python\Screperino\Log"
NUMERO_ESTRAZIONI_TEST = 17 # Quante estrazioni vuoi simulare
SECONDI_ATTESA = 30 # Secondi tra un'estrazione e l'altra

def setup_logging(log_path):
    """Configura il logging per il test."""
    os.makedirs(log_path, exist_ok=True)
    log_file = os.path.join(log_path, 'Log_Test_Rapido.txt')
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, mode='w', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def esegui_script(nome_script):
    """Esegue uno degli script del progetto."""
    percorso_script = os.path.join(PATH_PROGETTO, nome_script)
    if not os.path.exists(percorso_script):
        logging.error(f"SCRIPT NON TROVATO: Impossibile trovare '{nome_script}'.")
        return False
    try:
        logging.info(f"--- AVVIO SCRIPT: {nome_script} ---")
        subprocess.run([PYTHON_EXE, percorso_script], check=True, cwd=PATH_PROGETTO)
        logging.info(f"--- FINE SCRIPT: {nome_script} completato con successo. ---")
        return True
    except Exception as e:
        logging.error(f"ERRORE durante l'esecuzione di {nome_script}: {e}")
        return False

def avvia_test_rapido():
    """Simula un'intera giornata di esecuzioni in breve tempo."""
    setup_logging(PATH_LOG)
    logging.info("======================================================")
    logging.info("  AVVIO TEST RAPIDO DEL FLUSSO COMPLETO")
    logging.info(f"  Simuleremo {NUMERO_ESTRAZIONI_TEST} estrazioni ogni {SECONDI_ATTESA} secondi.")
    logging.info("======================================================")

    
    for i in range(NUMERO_ESTRAZIONI_TEST):
        logging.info(f"--> Esecuzione estrazione di test ({i + 1}/{NUMERO_ESTRAZIONI_TEST})")
        if not esegui_script("estrazione_giornaliera.py"):
            logging.critical("Estrazione fallita. Test interrotto.")
            return
        
        if i < NUMERO_ESTRAZIONI_TEST - 1:
            logging.info(f"Attesa di {SECONDI_ATTESA} secondi prima della prossima estrazione...\n")
            time.sleep(SECONDI_ATTESA)

    logging.info("\nSimulazione delle estrazioni completata.")
    logging.info("------------------------------------------------------")
    
    
    logging.info("--> Avvio del processo finale di pulizia e unione")
    esegui_script("pulisci_giornaliere.py")
    esegui_script("pulisci_bot.py")
    esegui_script("unione_finale.py")
    
    logging.info("\n======================================================")
    logging.info("  TEST RAPIDO COMPLETATO")
    logging.info("======================================================")

if __name__ == "__main__":
    avvia_test_rapido()