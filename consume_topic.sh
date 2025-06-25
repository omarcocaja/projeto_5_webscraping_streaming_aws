#!/bin/bash

# Nome do tópico a ser consumido (ex: bbc, reddit, reliefweb, emsc)
TOPIC_NAME=$1

# Endereço do broker
BROKER="localhost:9092"

# Validação de argumento
if [ -z "$TOPIC_NAME" ]; then
  echo "Você precisa informar o nome do tópico."
  echo "Exemplo de uso: ./consume_topic.sh bbc"
  exit 1
fi

echo "Consumindo mensagens do tópico '$TOPIC_NAME'..."
echo "Broker: $BROKER"
echo "Pressione CTRL+C para encerrar."

# Consome mensagens do início (para debug completo)
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server $BROKER \
  --topic $TOPIC_NAME \
  --from-beginning
