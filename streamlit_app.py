"""Punto de entrada de la app para Streamlit (Community Cloud y local).

Streamlit Community Cloud busca ``streamlit_app.py`` en la raiz del repositorio
(o el archivo indicado en "Main file path" del despliegue). Este wrapper deja
la app en la raiz y delega en ``src/dashboard_app.py``.

Para ejecutarla en local:

    python -m streamlit run streamlit_app.py
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from dashboard_app import main  # noqa: E402

if __name__ == "__main__":
    main()
