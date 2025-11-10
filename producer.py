from kafka import KafkaProducer
from const import *
import sys

if len(sys.argv) < 3:
    print("Uso: python3 produtor_grupo.py <grupo> <seu_nome>")
    exit(1)

topico = sys.argv[1]
user = sys.argv[2]  # nome do usuario no chat

# Criar produtor Kafka
produtor = KafkaProducer(bootstrap_servers=[BROKER_ADDR + ':' + BROKER_PORT])

print(f"Entrou no chat **{topico}** como {user} 😎")
print("Digite sua mensagem e ENTER para mandar (digite /sair pra sair)\n")

while True:
    txt = input(f"{user} >>> ")
    
    if txt == "/sair":
        print("Saiu")
        break
    
    msg = f"{user}: {txt}"
  
    produtor.send(topico, value=msg.encode())
    produtor.flush()
