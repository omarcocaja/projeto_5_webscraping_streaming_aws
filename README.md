# Pipeline de Webscraping em Streaming com AWS, Kafka e Spark

Este projeto implementa um pipeline de dados **em tempo real** que consome notícias e eventos de diferentes fontes via **webscraping**, publica os dados em tópicos Kafka e os armazena em uma arquitetura de **Data Lake** baseada na AWS, com estrutura de camadas **Bronze** e futura evolução para Silver/Gold.

O objetivo é simular um pipeline de ingestão contínua de dados não estruturados em um cenário de **monitoramento de notícias em tempo real**.

---

## 🗂 Estrutura do Projeto

```
projeto_5_webscraping_streaming_aws/
│
├── kafka_files/
│   ├── kafka_utils.py                   # Funções de utilidade para envio de mensagens ao Kafka
│
├── kafka_consumers/
│   └── emsc_consumer_to_s3.py          # Spark Consumer que escreve os dados da bronze no S3
│
├── web_sources/
│   ├── BBC/                            # Scraper para a BBC News (RSS/XML)
│   ├── ReliefWeb/                      # Scraper para atualizações humanitárias globais
│   └── Reddit/                         # Scraper para subreddits via RSS
│
├── 2 - Kafka/
│   └── write_to_kafka.py               # Função comum para todos os crawlers publicarem em Kafka
│
├── dev-s3/                             # Estrutura local simulando o bucket S3 (data lake)
│   └── teste-portfolio-projeto5/
│       └── datalake/
│           └── bronze/                 # Camada bronze com arquivos particionados por data e fonte
│
├── docker-compose.yaml                 # Sobe Kafka, Zookeeper e demais serviços de infraestrutura
├── requirements.txt                    # Dependências Python
├── consume_topic.sh                    # Script de debug para consumir dados de um tópico Kafka
└── README.md                           # Este arquivo
```

---

## ⚙️ Como Executar

### 1. Suba os serviços com Docker
```bash
docker-compose up -d
```

### 2. Execute os crawlers (localmente)
```bash
python web_sources/BBC/bbc_scraper.py
```

> Os dados são publicados em tópicos Kafka automaticamente com a função `write_to_kafka`.

### 3. Execute o consumidor Spark
```bash
spark-submit kafka_consumers/emsc_consumer_to_s3.py
```

> O consumer escreve os dados da camada bronze em arquivos `.parquet` no S3 simulado (`dev-s3/`).

---

## 📦 Tecnologias Utilizadas

- **Kafka + Zookeeper**
- **Apache Spark Structured Streaming**
- **Python (Playwright, feedparser, boto3)**
- **Docker e Docker Compose**
- **AWS S3 (simulado localmente)**
- **AWS**
- **Formato Parquet**

---

## 📄 Licença

Este projeto está licenciado sob a licença **MIT** e pode ser livremente utilizado e adaptado.

---

## 📬 Contato

- [LinkedIn](https://www.linkedin.com/in/marco-caja)  
- [Instagram](https://www.instagram.com/omarcocaja)
