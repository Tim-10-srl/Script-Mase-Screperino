# bootstrap.py
"""
Bootstrap per i progetti Screperino e MASE.
Si occupa di aggiungere automaticamente la cartella 'Sviluppo'
al sys.path, così che i moduli condivisi (config, ecc.)
siano sempre importabili da ogni sottocartella.
"""

import sys
import os

# Percorso assoluto della cartella Sviluppo (due livelli sopra lo script corrente)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))

if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
