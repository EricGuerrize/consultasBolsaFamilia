"""
Conector Firebase — upload de cruzamentos para Firestore.

Setup:
  1. No Console Firebase > Configurações do Projeto > Contas de serviço
     clique em "Gerar nova chave privada" e salve o JSON baixado.
  2. Defina no .env:  FIREBASE_SERVICE_ACCOUNT_PATH=caminho/para/chave.json
"""

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _get_firestore():
    """Import lazy para não quebrar em ambientes sem firebase-admin instalado."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
        return firebase_admin, credentials, firestore
    except ImportError:
        raise ImportError(
            "Pacote firebase-admin não instalado. Execute:\n"
            "  pip install firebase-admin"
        )


class FirebaseConnector:
    def __init__(self):
        service_account_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", "")
        if not service_account_path:
            raise ValueError(
                "FIREBASE_SERVICE_ACCOUNT_PATH não definido no .env\n"
                "Exemplo: FIREBASE_SERVICE_ACCOUNT_PATH=firebase_service_account.json"
            )
        if not Path(service_account_path).exists():
            raise FileNotFoundError(
                f"Arquivo de credenciais não encontrado: {service_account_path}\n"
                "Baixe em: Console Firebase › Configurações do Projeto › "
                "Contas de serviço › Gerar nova chave privada"
            )

        firebase_admin, credentials_mod, firestore_mod = _get_firestore()
        if not firebase_admin._apps:
            cred = credentials_mod.Certificate(service_account_path)
            firebase_admin.initialize_app(cred)

        self._firestore = firestore_mod
        self.db = firestore_mod.client()

    def upload_cruzamento(self, registros: list[dict], run_id: str = None) -> str:
        """
        Faz upload em batch para Firestore.
        Estrutura: cruzamentos/{run_id}/registros/{auto_id}
        Retorna o run_id usado.
        """
        if not run_id:
            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        col_ref = (
            self.db.collection("cruzamentos")
            .document(run_id)
            .collection("registros")
        )

        # Firestore suporta no máximo 500 operações por batch
        BATCH_LIMIT = 499
        batch = self.db.batch()
        ops_no_batch = 0
        total = 0

        for reg in registros:
            doc_ref = col_ref.document()
            # Sanitiza: remove None e converte floats inválidos
            reg_clean = {}
            for k, v in reg.items():
                if v is None:
                    reg_clean[k] = ""
                elif isinstance(v, float) and (v != v):  # NaN check sem math
                    reg_clean[k] = 0.0
                else:
                    reg_clean[k] = v
            batch.set(doc_ref, reg_clean)
            ops_no_batch += 1
            total += 1

            if ops_no_batch >= BATCH_LIMIT:
                batch.commit()
                batch = self.db.batch()
                ops_no_batch = 0

        if ops_no_batch > 0:
            batch.commit()

        return run_id

    def salvar_metadados_run(self, run_id: str, meta: dict):
        """Persiste metadados do run (merge para não sobrescrever registros)."""
        self.db.collection("cruzamentos").document(run_id).set(meta, merge=True)
