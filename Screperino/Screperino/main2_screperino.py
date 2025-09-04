import os, sys, time, subprocess, logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta, date
import bootstrap

from config import (
    PYTHON_A, PYTHON_B, A_DIR, B_FILE, LOG_DIR, LAST_B, LAST_EOD,
    EOD_HOUR, EOD_MIN, B_EVERY_H, A_MINUTE, ensure_dirs
)

# prepara log-dir
ensure_dirs()


# Crea la cartella di log se non esiste
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("orchestratore")
logger.setLevel(logging.INFO)
fh = RotatingFileHandler(os.path.join(LOG_DIR, "orchestratore.log"),
                         maxBytes=2_000_000, backupCount=3, encoding="utf-8")
fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
fh.setFormatter(fmt)
logger.addHandler(fh)
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(fmt)
logger.addHandler(sh)

# ===== Util per subprocess con log =====
def run_and_log(cmd, cwd):
    """
    Esegue un comando, logga stdout/stderr e solleva CalledProcessError se returncode != 0.
    """
    logger.info(f"Eseguo: {cmd} (cwd={cwd})")
    res = subprocess.run(cmd, cwd=cwd, text=True, capture_output=True)
    if res.stdout:
        logger.info(res.stdout.strip())
    if res.stderr:
        logger.error(res.stderr.strip())
    if res.returncode != 0:
        raise subprocess.CalledProcessError(res.returncode, cmd, output=res.stdout, stderr=res.stderr)

# ===== Persistenza ultimi run =====
def read_last_b():
    try:
        with open(LAST_B, "r", encoding="utf-8") as f:
            return datetime.fromisoformat(f.read().strip())
    except Exception:
        return None

def write_last_b(ts: datetime):
    with open(LAST_B, "w", encoding="utf-8") as f:
        f.write(ts.isoformat())

def read_last_eod():
    """Ritorna la *data* (date) dell’ultimo EOD eseguito, oppure None."""
    try:
        with open(LAST_EOD, "r", encoding="utf-8") as f:
            return datetime.fromisoformat(f.read().strip()).date()
    except Exception:
        return None

def write_last_eod_for_day(day: date):
    """
    Salva nel file LAST_EOD la data del giorno *processato* dall’EOD.
    Questo permette il catch-up di ieri senza 'bruciare' l’EOD di oggi.
    """
    with open(LAST_EOD, "w", encoding="utf-8") as f:
        ts = datetime.combine(day, datetime.min.time())
        f.write(ts.isoformat())

# ===== EOD =====
def run_eod_for_date(target_date: date):
    """
    Esegue gli script EOD per una data specifica (oggi o catch-up di ieri).
    Passa la data a pulisci_giornaliera.py come 'YYYY-MM-DD'.
    """
    try:
        logger.info(f"EOD: avvio pulizia/unione per la data {target_date.isoformat()}.")
        # pulizia giornaliera con data specifica (il tuo script la supporta)
        run_and_log([PYTHON_A, os.path.join(A_DIR, "pulisci_giornaliera.py"), target_date.isoformat()], A_DIR)

        # gli altri due come prima (se un giorno servirà la data, estendili in modo analogo)
        run_and_log([PYTHON_A, os.path.join(A_DIR, "pulisci_bot.py"), target_date.isoformat()], A_DIR)
        run_and_log([PYTHON_A, os.path.join(A_DIR, "unione_finale.py"), target_date.isoformat()], A_DIR)

        # scrivo la *data processata* (non l'istante di esecuzione)
        write_last_eod_for_day(target_date)
        logger.info(f"EOD completato per la data {target_date.isoformat()}.")
    except subprocess.CalledProcessError as e:
        logger.error(f"EOD: script fallito (rc={e.returncode}) -> {e.cmd}")
    except Exception as e:
        logger.error(f"EOD: errore inatteso: {e}")

def maybe_run_eod():
    """
    Esegue i job di fine giornata alle EOD_HOUR:EOD_MIN, una sola volta per giorno.
    - Catch-up: se ieri non è stato fatto, lo esegue subito al primo giro utile.
    - Se è prima della finestra EOD, ritorna (tranne 23:00–EOD_MIN-1: attende fino a EOD_MIN).
    """
    try:
        today = datetime.now().date()
        last  = read_last_eod()
        now   = datetime.now()
        logger.info(f"EOD: stato -> now={now.strftime('%Y-%m-%d %H:%M:%S')}, last={last}, today={today}")

        yesterday = today - timedelta(days=1)

        # --- CATCH-UP di ieri ---
        if last is None or last < yesterday:
            logger.info(f"EOD: catch-up rilevato. Ultimo EOD processato: {last}. Eseguo per {yesterday}.")
            run_eod_for_date(yesterday)
            # dopo il catch-up NON ritorno: lascio che l'EOD di oggi avvenga più tardi alla finestra prevista

        # se l'ultimo EOD è proprio oggi, ho già fatto quello di oggi
        last = read_last_eod()  # rileggo nel caso il catch-up l'abbia aggiornato
        if last == today:
            logger.info("EOD: già eseguito oggi. Skip.")
            return

        # --- Finestra EOD di oggi ---
        reached = (now.hour > EOD_HOUR) or (now.hour == EOD_HOUR and now.minute >= EOD_MIN)
        if not reached:
            # attendo solo se siamo nella finestra 23:00..EOD_MIN-1
            if now.hour == EOD_HOUR and now.minute < EOD_MIN:
                target = now.replace(hour=EOD_HOUR, minute=EOD_MIN, second=0, microsecond=0)
                delta  = (target - now).total_seconds()
                logger.info(f"EOD: attendo fino alle {target.strftime('%H:%M')} (~{int(delta)}s) prima di eseguire.")
                time.sleep(max(0, delta))
            else:
                logger.info("EOD: troppo presto, ritorno e riprovo al prossimo ciclo.")
                return

        # se arrivo qui, finestra raggiunta (o attesa finita): eseguo oggi
        logger.info("EOD: finestra raggiunta per oggi. Avvio EOD giorno corrente.")
        run_eod_for_date(today)

    except Exception as e:
        logger.error(f"EOD: errore inatteso in maybe_run_eod: {e}")

# ===== Scheduling A/B =====
def sleep_until_next_A():
    now = datetime.now().replace(second=0, microsecond=0)
    # Se non abbiamo ancora superato il minuto A_MINUTE in questa ora, usa questa ora.
    if now.minute < A_MINUTE:
        next_run = now.replace(minute=A_MINUTE)
    else:
        # Altrimenti vai all'ora successiva al minuto A_MINUTE
        next_run = (now + timedelta(hours=1)).replace(minute=A_MINUTE)
    delta = (next_run - datetime.now()).total_seconds()
    logger.info(f"Sleep fino a {next_run.strftime('%d-%m %H:%M:%S')} (~{int(delta)}s)")
    time.sleep(max(0, delta))

def run_A_once():
    # Esegue l’equivalente di compito_estrazione_oraria()
    cmd = [PYTHON_A, os.path.join(A_DIR, "estrazione_giornaliera.py")]
    logger.info(f"Eseguo A: {cmd}")
    try:
        subprocess.run(cmd, cwd=A_DIR, check=True)
        logger.info("A OK")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"A exit {e.returncode}")
    except Exception as e:
        logger.error(f"A errore: {e}")
    return False

def run_B_once():
    # Richiede che il gestore B supporti --once e usi rc=111 per 'SKIPPED (LOCK)'
    cmd = [PYTHON_B, B_FILE, "--once"]
    logger.info(f"Eseguo B (once): {cmd}")
    try:
        res = subprocess.run(cmd, cwd=os.path.dirname(B_FILE), text=True, capture_output=True)
        if res.stdout:
            logger.info(res.stdout.strip())
        if res.stderr:
            logger.error(res.stderr.strip())

        rc = res.returncode
        if rc == 0:
            logger.info("B DONE")
            write_last_b(datetime.now())  # aggiorna SOLO se ha eseguito davvero
            return True
        elif rc == 111:
            logger.info("B SKIPPED (LOCK)")
            return False
        else:
            logger.error(f"B ERROR rc={rc}")
            return False
    except Exception as e:
        logger.error(f"B errore: {e}")
        return False

def should_run_B():
    last = read_last_b()
    if last is None:
        return True
    return datetime.now() - last >= timedelta(hours=B_EVERY_H)

def sanity_checks():
    assert os.path.isdir(A_DIR), f"A_DIR non esiste: {A_DIR}"
    assert os.path.isfile(os.path.join(A_DIR, "estrazione_giornaliera.py")), \
        f"Manca estrazione_giornaliera.py in {A_DIR}"
    assert os.path.isfile(B_FILE), f"GESTORE B non trovato: {B_FILE}"
    assert os.path.isfile(PYTHON_A), f"Python A non trovato: {PYTHON_A}"
    assert os.path.isfile(PYTHON_B), f"Python B non trovato: {PYTHON_B}"
    os.makedirs(LOG_DIR, exist_ok=True)

def main():
    sanity_checks()
    logger.info("=== Orchestratore LEAN avviato (h24/7) ===")

    # Se avvii dopo la finestra EOD, fa subito l'EOD (e catch-up se serve)
    maybe_run_eod()

    while True:
        try:
            sleep_until_next_A()
            okA = run_A_once()

            # dopo A, se servono le 4h lancia B
            if okA and should_run_B():
                logger.info("Sono passate >= 4h dall'ultimo B: avvio B.")
                run_B_once()

            # dopo A/B valuta l'EOD (e se sei tra 23:00 e EOD_MIN-1, attende fino all'orario EOD)
            maybe_run_eod()

            # loop continua: il processo dormirà fino al prossimo slot
        except KeyboardInterrupt:
            logger.info("Interrotto dall’utente. Chiusura ordinata.")
            break
        except Exception as e:
            logger.error(f"Errore inatteso in main loop: {e}")
            time.sleep(30)

if __name__ == "__main__":
    main()