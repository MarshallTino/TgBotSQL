import time
from utils.db_mongo import get_pending_dexscreener_data, mark_as_processed
from utils.db_postgres import connect_postgres

def transfer_dexscreener_data():
    pending_data = get_pending_dexscreener_data("dexscreener_raw")
    if not pending_data:
        print("ℹ️ No hay datos pendientes en MongoDB.")
        return

    conn = connect_postgres()
    cursor = conn.cursor()

    doc_ids = []
    for doc in pending_data:
        token_address = doc.get("token_address")
        blockchain = doc.get("blockchain")
        price_usd = doc.get("priceUsd", "0")

        # Actualizar el token en PostgreSQL (por ahora solo call_price como ejemplo)
        cursor.execute("""
            UPDATE tokens
            SET call_price = %s
            WHERE contract_address = %s AND blockchain = %s
        """, (price_usd, token_address, blockchain))

        doc_ids.append(doc["_id"])

    conn.commit()
    conn.close()

    # Marcar como procesados en MongoDB
    mark_as_processed("dexscreener_raw", doc_ids)
    print(f"✅ Procesados {len(doc_ids)} registros de MongoDB a PostgreSQL.")

if __name__ == "__main__":
    print("🚀 Iniciando transferencia MongoDB → PostgreSQL...")
    while True:
        try:
            transfer_dexscreener_data()
            print("⏳ Esperando 5 minutos antes del próximo ciclo...")
            time.sleep(300)  # 5 minutos
        except Exception as e:
            print(f"❌ Error en transferencia: {e}")
            time.sleep(60)  # Esperar 1 minuto antes de reintentar
