# 🚀 Open Financial Lakehouse

## 🎯 Objetivo do Projeto
Este projeto implementa uma **Modern Data Stack (MDS)** completa para o mercado financeiro. O objetivo é transformar dados brutos da API `yfinance` em ativos prontos para análise quantitativa, utilizando a **Arquitetura Medallion**. O pipeline é focado em idempotência, garantindo que reprocessamentos não gerem duplicidade e que a linhagem dos dados seja auditável. Possui um front-end interativo simples e basico apenas para testar o processamento.

## 🛠️ Tecnologias Utilizadas
- **Processamento:** Apache Spark 4.1 (WSL2/Ubuntu)
- **Armazenamento:** Delta Lake (Transações ACID e Time Travel)
- **Governança & Transformação:** dbt (Data Build Tool) - Camadas Silver e Gold
- **Ingestão:** Python & yfinance API
- **Motor de Decisão:** Scripts Python customizados integrados ao Spark (Inspiration FICO Blaze)

## 🏗️ Arquitetura e Decisões Técnicas
- **Medallion Architecture**: Dados organizados nas camadas Bronze (Raw/CSV), Silver (Tratamento/Delta) e Gold (Agregações/Negócio).
- **Ambiente WSL2**: Devido a restrições de permissões POSIX em sistemas de arquivos híbridos, o projeto foi otimizado para execução dentro do sistema de arquivos nativo do Linux (Ext4) no WSL2.
- **Idempotência**: O pipeline permite reexecuções sem duplicação de dados, utilizando o controle de transações do Delta Lake e limpezas de cache físico (`shutil.rmtree`).
- **Decision Intelligence**: Implementação de um motor de regras para sinais de compra e venda baseado em médias móveis (MME21) e análise de exaustão de volume.

## 📂 Estrutura do Projeto
- `analytics/`: Projeto dbt contendo modelos SQL, testes de dados e contratos.
- `data/bronze/`: Armazenamento dos arquivos CSV brutos da ingestão.
- `lakehouse_data/`: Armazenamento oficial das tabelas Delta (Bronze consolidada e Warehouse Gold).
- `scripts/`: Orquestradores, extratores e o motor de decisão.
- `docker-compose.yml`: Infraestrutura de apoio para serviços de governança.

## 🚀 Setup e Execução no WSL2

### 1. Requisitos do Sistema (WSL2)
Certifique-se de ter o Java 17 instalado no seu Ubuntu:
```bash
sudo apt update
sudo apt install openjdk-17-jdk -y
```

### 2. Clonar e Preparar Ambiente
Dentro do terminal do seu WSL2 (Ubuntu):
```bash
git clone <url-do-seu-repositorio>
cd open_financial_lakehouse
python3 -m venv .venv_linux
source .venv_linux/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configuração do dbt
O dbt requer a instalação dos pacotes de macros antes da primeira execução:
```bash
cd analytics
dbt deps
cd ..
```

### 4. Execução do Front-End
Ativas o Front para testes
```bash
cd api
uvicorn main:app --reload
```