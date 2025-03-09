import time
from utils.db_postgres import connect_postgres
from utils.db_mongo import insert_dexscreener_data
from utils.api_clients import get_pairs_data

def fetch_dexscreener_data():
    conn = connect_postgres()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT contract_address, blockchain FROM tokens WHERE contract_address IS NOT NULL")
    tokens = cursor.fetchall()
    conn.close()

    for contract_address, blockchain in tokens:
        pairs = get_pairs_data(blockchain, [contract_address])
        if pairs:
            for pair in pairs:
                pair["token_address"] = contract_address
                pair["blockchain"] = blockchain
                pair["fetched_at"] = int(time.time())
                pair["processed"] = False
            insert_dexscreener_data("dexscreener_raw", pairs)
        time.sleep(1)  # Evitar sobrecarga en la API

if __name__ == "__main__":
    print("🚀 Iniciando fetcher de Dexscreener...")
    while True:
        try:
            fetch_dexscreener_data()
            print("⏳ Esperando 5 minutos antes del próximo fetch...")
            time.sleep(300)  # 5 minutos
        except Exception as e:
            print(f"❌ Error en fetcher: {e}")
            time.sleep(60)  # Esperar 1 minuto antes de reintentar
