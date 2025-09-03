import subprocess
import time
import sys
import os
import argparse

# --- CONFIGURAZIONE ---
PYTHON_EXE = sys.executable
PATH_CARTELLA_SCRIPT = os.path.dirname(os.path.abspath(__file__))  # ...\Mase\script
PATH_SCRIPT_1 = os.path.join(PATH_CARTELLA_SCRIPT, "screp.py")
PATH_SCRIPT_2 = os.path.join(PATH_CARTELLA_SCRIPT, "elaboratore.py")
INTERVALLO_SECONDI = 4 * 60 * 60  # resident: ogni 4 ore
LOCK = os.path.join(PATH_CARTELLA_SCRIPT, "mase.lock")
LOCK_TTL_SEC = 5 * 60 * 60  # 5 ore: B gira ogni 4h, oltre 5h il lock è stale

def sanity_checks():
    assert os.path.isdir(PATH_CARTELLA_SCRIPT), f"Cartella script non esiste: {PATH_CARTELLA_SCRIPT}"
    assert os.path.isfile(PATH_SCRIPT_1), f"Manca screp.py in {PATH_CARTELLA_SCRIPT}"
    assert os.path.isfile(PATH_SCRIPT_2), f"Manca elaboratore.py in {PATH_CARTELLA_SCRIPT}"
    assert os.path.isfile(PYTHON_EXE), f"Python non trovato: {PYTHON_EXE}"

def acquire_lock():
    # se il lock esiste ma è STALE (vecchio), lo rimuovo
    if os.path.exists(LOCK):
        age = time.time() - os.path.getmtime(LOCK)
        if age > LOCK_TTL_SEC:
            try:
                os.remove(LOCK)
                print(f"Lock stale rimosso (età {int(age)}s).")
            except Exception as e:
                print(f"Lock stale ma non rimovibile: {e}")
                return False
        else:
            return False

    # creazione atomica del lock
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

def run_one_cycle():
    print(f"\n[FASE 1] Avvio di 'screp.py'...")
    subprocess.run([PYTHON_EXE, PATH_SCRIPT_1], check=True, cwd=PATH_CARTELLA_SCRIPT)
    print(f"[FASE 1] '{os.path.basename(PATH_SCRIPT_1)}' completato con successo.")

    time.sleep(10)

    print(f"\n[FASE 2] Avvio di 'elaboratore.py'...")
    subprocess.run([PYTHON_EXE, PATH_SCRIPT_2], check=True, cwd=PATH_CARTELLA_SCRIPT)
    print(f"[FASE 2] '{os.path.basename(PATH_SCRIPT_2)}' completato con successo.")

def main():
    sanity_checks()

    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Esegue una sola corsa e termina")
    args = parser.parse_args()

    print("--- GESTORE MASE AVVIATO ---")

    # Modalità orchestrata (chiamata da A)
    if args.once:
        if not acquire_lock():
            print("Lock presente: B sembra già in esecuzione. Esco.")
            sys.exit(111)  # rc=111: SKIPPED per lock
        try:
            run_one_cycle()
        except subprocess.CalledProcessError as e:
            print(f"!!! ERRORE: Uno degli script ha terminato con un errore: {e}")
            sys.exit(1)
        except FileNotFoundError:
            print("!!! ERRORE: File non trovato. Controlla i percorsi degli script.")
            sys.exit(1)
        except Exception as e:
            print(f"!!! ERRORE INASPETTATO nel gestore: {e}")
            sys.exit(1)
        finally:
            release_lock()
        print("\n--- CORSA SINGOLA COMPLETATA ---")
        return

    # Modalità resident (stand-alone, ogni 4 ore)
    if not acquire_lock():
        print("Lock presente: B già in esecuzione altrove. Esco.")
        return

    try:
        print(f"Il processo verrà eseguito ogni {INTERVALLO_SECONDI / 3600} ore.")
        print("ATTENZIONE: Non chiudere questa finestra del terminale.")
        ciclo_numero = 0
        while True:
            ciclo_numero += 1
            print(f"\n--- INIZIO CICLO #{ciclo_numero} ({time.strftime('%d/%m/%Y %H:%M:%S')}) ---")
            try:
                run_one_cycle()
            except subprocess.CalledProcessError as e:
                print(f"!!! ERRORE: Uno degli script ha terminato con un errore: {e}")
            except FileNotFoundError:
                print("!!! ERRORE: File non trovato. Controlla i percorsi degli script.")
                break
            except Exception as e:
                print(f"!!! ERRORE INASPETTATO nel gestore: {e}")

            # heartbeat sul lock: aggiorna mtime per indicare che il processo è vivo
            try:
                os.utime(LOCK, None)
            except Exception:
                pass

            print(f"\n--- CICLO #{ciclo_numero} COMPLETATO ---")
            ora_prossima = time.strftime('%H:%M:%S', time.localtime(time.time() + INTERVALLO_SECONDI))
            print(f"Prossima esecuzione tra {INTERVALLO_SECONDI / 3600} ore (alle {ora_prossima})")
            time.sleep(INTERVALLO_SECONDI)
    finally:
        release_lock()

if __name__ == "__main__":
    main()