# Open Financial Lakehouse

## Objetivo do Projeto
Este projeto implementa uma plataforma de dados moderna (Modern Data Stack) para transformar dados brutos do mercado financeiro em inteligência de decisão com inserção automatizada de dados. O foco principal é a governança, auditabilidade e a construção de um pipeline fim-a-fim que alimenta um motor de decisão quantitativo.

## Tecnologias Utilizadas
- Processamento: Apache Spark 4.1 (WSL2/Ubuntu)
- Armazenamento: Delta Lake (Transações ACID e Time Travel)
- Governança: dbt (Data Build Tool) e Metastore Local
- Transformação: dbt (Camadas Silver e Gold)
- Ingestão: Python & yfinance API
- Motor de Decisão: Scripts Python customizados integrados ao Spark (Inspiration FICO Blaze)

## Arquitetura e Decisões Técnicas
- **Medallion Architecture**: Dados organizados nas camadas Bronze (Raw/CSV), Silver (Tratamento/Delta) e Gold (Agregações/Negócio).
- **Ambiente WSL2**: Devido a restrições de permissões POSIX em sistemas de arquivos híbridos, o projeto foi otimizado para execução dentro do sistema de arquivos nativo do Linux (Ext4) no WSL2.
- **Idempotência**: O pipeline permite reexecuções sem duplicação de dados, utilizando o controle de transações do Delta Lake.
- **Decision Intelligence**: Implementação de um motor de regras para sinais de compra e venda baseado em médias móveis (MME21) e análise de exaustão de volume.

## Estrutura do Projeto
- `analytics/`: Projeto dbt contendo modelos SQL, testes de dados e contratos.
- `data/`: Armazenamento local das camadas Bronze, Silver e Gold em formato Delta.
- `scripts/`: Orquestradores, extratores e o motor de decisão.
- `docker-compose.yml`: Infraestrutura de apoio para serviços de governança.

## Setup e Execução no WSL2

### 1. Requisitos do Sistema (WSL2)
Certifique-se de ter o Java 17 instalado no seu Ubuntu:
```bash
sudo apt update
```
```bash
sudo apt install openjdk-17-jdk
```

### 2. Clonar e Preparar Ambiente
Dentro do terminal do seu WSL2 (Ubuntu):
```bash
git clone <url-do-seu-repositorio>
```
```bash
cd open_financial_lakehouse
```
```bash
python3 -m venv .venv_linux
```
```bash
source .venv_linux/bin/activate
```
```bash
pip install --upgrade pip
```
```bash
pip install -r requirements.txt
```

### Iniciando  o dbt
```bash
cd analytics
```
```bash
dbt deps
```
```bash
cd ..
```

### Execução do pipeline
```bash
python3 scripts/event_orchestrator.py
```