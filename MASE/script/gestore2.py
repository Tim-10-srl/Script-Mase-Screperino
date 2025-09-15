import subprocess
import time
import sys
import os
import argparse
import logging
from logging.handlers import RotatingFileHandler
from config import LOG_DIR

# --- CONFIGURAZIONE ---
PYTHON_EXE = sys.executable
PATH_CARTELLA_SCRIPT = os.path.dirname(os.path.abspath(__file__))  # ...\Mase\script
PATH_SCRIPT_1 = os.path.join(PATH_CARTELLA_SCRIPT, "screp.py")
PATH_SCRIPT_2 = os.path.join(PATH_CARTELLA_SCRIPT, "elaboratore.py")
INTERVALLO_SECONDI = 4 * 60 * 60  # resident: ogni 4 ore
LOCK = os.path.join(PATH_CARTELLA_SCRIPT, "mase.lock")
LOCK_TTL_SEC = 5 * 60 * 60  # 5 ore: B gira ogni 4h, oltre 5h il lock è stale

# --- LOG B (MASE) ---
os.makedirs(LOG_DIR, exist_ok=True)
logger_b = logging.getLogger("mase")
logger_b.setLevel(logging.INFO)

if not logger_b.handlers:
    fh = RotatingFileHandler(os.path.join(LOG_DIR, "mase.log"),
                             maxBytes=2_000_000, backupCount=3, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger_b.addHandler(fh)
    logger_b.addHandler(sh)


def sanity_checks():
    assert os.path.isdir(PATH_CARTELLA_SCRIPT), f"Cartella script non esiste: {PATH_CARTELLA_SCRIPT}"
    assert os.path.isfile(PATH_SCRIPT_1), f"Manca screp.py in {PATH_CARTELLA_SCRIPT}"
    assert os.path.isfile(PATH_SCRIPT_2), f"Manca elaboratore.py in {PATH_CARTELLA_SCRIPT}"
    assert os.path.isfile(PYTHON_EXE), f"Python non trovato: {PYTHON_EXE}"


def acquire_lock():
    if os.path.exists(LOCK):
        age = time.time() - os.path.getmtime(LOCK)
        if age > LOCK_TTL_SEC:
            try:
                os.remove(LOCK)
                logger_b.info(f"Lock stale rimosso (età {int(age)}s).")
            except Exception as e:
                logger_b.error(f"Lock stale ma non rimovibile: {e}")
                return False
        else:
            return False

    try:
        fd = os.open(LOCK, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        with os.fdopen(fd, "w") as f:
            f.write(f"pid={os.getpid()} ts={int(time.time())}")
        return True
    except FileExistsError:
        return False


def release_lock():
    try:
        os.remove(LOCK)
    except FileNotFoundError:
        pass


def _run_and_stream(cmd, cwd, prefix):
    # forza Python unbuffered nei figli
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    cmd = [cmd[0], "-u"] + cmd[1:]  # aggiunge -u a PYTHON_EXE

    logger_b.info(f"[{prefix}] Avvio comando: {cmd}")
    with subprocess.Popen(cmd, cwd=cwd, text=True, encoding="utf-8", errors="replace",
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          env=env, bufsize=1) as p:
        for line in p.stdout:
            logger_b.info(f"[{prefix}] {line.rstrip()}")
        rc = p.wait()
    if rc != 0:
        logger_b.error(f"[{prefix}] Terminato con codice {rc}")
        raise subprocess.CalledProcessError(rc, cmd)


def run_one_cycle():
    _run_and_stream([PYTHON_EXE, PATH_SCRIPT_1], PATH_CARTELLA_SCRIPT, "screp")
    time.sleep(10)
    _run_and_stream([PYTHON_EXE, PATH_SCRIPT_2], PATH_CARTELLA_SCRIPT, "elaboratore")


def main():
    sanity_checks()

    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Esegue una sola corsa e termina")
    args = parser.parse_args()

    logger_b.info("--- GESTORE MASE AVVIATO ---")

    if args.once:
        if not acquire_lock():
            logger_b.info("Lock presente: B sembra già in esecuzione. Esco.")
            sys.exit(111)
        try:
            run_one_cycle()
        except subprocess.CalledProcessError as e:
            logger_b.error(f"!!! ERRORE: Uno degli script ha terminato con un errore: {e}")
            sys.exit(1)
        except FileNotFoundError:
            logger_b.error("!!! ERRORE: File non trovato. Controlla i percorsi degli script.")
            sys.exit(1)
        except Exception as e:
            logger_b.error(f"!!! ERRORE INASPETTATO nel gestore: {e}")
            sys.exit(1)
        finally:
            release_lock()
        logger_b.info("--- CORSA SINGOLA COMPLETATA ---")
        return

    if not acquire_lock():
        logger_b.info("Lock presente: B già in esecuzione altrove. Esco.")
        return

    try:
        logger_b.info(f"Il processo verrà eseguito ogni {INTERVALLO_SECONDI / 3600} ore.")
        ciclo_numero = 0
        while True:
            ciclo_numero += 1
            logger_b.info(f"--- INIZIO CICLO #{ciclo_numero} ({time.strftime('%d/%m/%Y %H:%M:%S')}) ---")
            try:
                run_one_cycle()
            except subprocess.CalledProcessError as e:
                logger_b.error(f"!!! ERRORE: Uno degli script ha terminato con un errore: {e}")
            except FileNotFoundError:
                logger_b.error("!!! ERRORE: File non trovato. Controlla i percorsi degli script.")
                break
            except Exception as e:
                logger_b.error(f"!!! ERRORE INASPETTATO nel gestore: {e}")

            try:
                os.utime(LOCK, None)
            except Exception:
                pass

            logger_b.info(f"--- CICLO #{ciclo_numero} COMPLETATO ---")
            ora_prossima = time.strftime('%H:%M:%S', time.localtime(time.time() + INTERVALLO_SECONDI))
            logger_b.info(f"Prossima esecuzione tra {INTERVALLO_SECONDI / 3600} ore (alle {ora_prossima})")
            time.sleep(INTERVALLO_SECONDI)
    finally:
        release_lock()


if __name__ == "__main__":
    main()
