from kafka import KafkaConsumer
from const import *
import sys

if len(sys.argv) < 2:
    print("Use assim: python3 consumidor_grupo.py <nome_do_grupo>")
    exit(1)

topico = sys.argv[1]

consumer = KafkaConsumer(
    topico,
    bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT],
    auto_offset_reset='earliest'
)

print(f"Conectado no grupo: {topico} ✅")
print("Esperando mensagem\n")

for msg in consumer:
    try:
        print(msg.value.decode())
    except:
        print("Erro ao decodificar")
