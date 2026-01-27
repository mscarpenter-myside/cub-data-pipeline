# CUB Data Pipeline

Pipeline automatizado de ETL para extração de dados do **Custo Unitário Básico (CUB)** dos sites dos Sinduscons estaduais brasileiros.

[![CUB Monthly Scraper](https://github.com/mscarpenter-myside/cub-vb/actions/workflows/scraper_schedule.yml/badge.svg)](https://github.com/YOUR_USERNAME/cub-vb/actions/workflows/scraper_schedule.yml)

---

## 🎯 O que o projeto faz

O **CUB Data Pipeline** é uma ferramenta de automação focada em processos de ETL (Extract, Transform, Load) e Monitoramento. O objetivo principal é:

- **Auditar a disponibilidade** de dados CUB nos sites dos Sinduscons
- **Coletar dados** de **14 estados** + **INCC-M** (nacional)
- **Validar** com Pydantic (Type Safety)
- **Exportar** CSV compilado para análise

### Estados Cobertos

| Estado | Método | Estado | Método |
|--------|--------|--------|--------|
| SC | HTML | GO | PDF |
| SP | HTML | RJ | PDF |
| PR | PDF | ES | PDF |
| MG | PDF | PE | PDF |
| RS | PDF | DF | PDF |
| MT | PDF | MA | PDF |
| PA | PDF | BR (INCC) | PDF |

---

## 📋 Pré-requisitos

- **Python 3.10+**
- **Playwright** (browser automation)
- **Git**

---

## 🚀 Instalação

```bash
# 1. Clonar o repositório
git clone https://github.com/YOUR_USERNAME/cub-vb.git
cd cub-vb

# 2. Criar ambiente virtual
python -m venv venv

# 3. Ativar ambiente virtual
# Linux/Mac:
source venv/bin/activate
# Windows:
venv\Scripts\activate

# 4. Instalar dependências
pip install -r requirements.txt

# 5. Instalar browser do Playwright
playwright install chromium
```

---

## ▶️ Como Executar

### Modo Automático (CI/CD)
Calcula automaticamente o mês anterior como target:
```bash
python -m src.main --auto
```

### Modo Manual
Especifica mês e ano diretamente:
```bash
python -m src.main 12 2025
```

### Filtrar Estados
Executa apenas para estados específicos:
```bash
python -m src.main --auto --states SC SP PR MG
```

### Ver todas as opções
```bash
python -m src.main --help
```

---

## 📁 Estrutura de Pastas

```
cub-vb/
├── .github/
│   └── workflows/
│       └── scraper_schedule.yml   # CI/CD (GitHub Actions)
├── data/
│   ├── raw/                       # PDFs baixados (temporário)
│   └── output/                    # CSVs gerados (resultado final)
├── src/
│   ├── main.py                    # Orquestrador principal
│   ├── core/
│   │   └── models.py              # Modelos Pydantic (CUBData, CUBValor)
│   ├── scrapers/
│   │   ├── base.py                # Interface abstrata (Strategy Pattern)
│   │   ├── sc.py                  # Scraper Santa Catarina
│   │   ├── sp.py                  # Scraper São Paulo
│   │   └── ...                    # Demais estados
│   └── utils/
│       └── helpers.py             # Funções auxiliares
├── .env.example                   # Template de variáveis de ambiente
├── .gitignore                     # Arquivos ignorados pelo Git
├── requirements.txt               # Dependências (versões pinadas)
└── README.md                      # Este arquivo
```

---

## ⚙️ CI/CD (GitHub Actions)

O pipeline roda **automaticamente** via GitHub Actions nos seguintes dias de cada mês:

| Dias | Horário UTC | Horário BRT |
|------|-------------|-------------|
| 1, 5, 10, 15, 20, 25, 29 | 12:00 | 09:00 |

### Fluxo de Execução

1. **Trigger**: CRON schedule ou dispatch manual
2. **Setup**: Python 3.11 + Playwright Chromium
3. **Execução**: `python -m src.main --auto`
4. **Auto-commit**: Se houver dados novos, commita o CSV automaticamente

### Executar Manualmente

No GitHub, vá em **Actions** → **CUB Monthly Scraper** → **Run workflow**

---

## 🏗️ Arquitetura

O projeto implementa o **Strategy Pattern** para lidar com a heterogeneidade das fontes:

```
┌─────────────────┐
│   main.py       │  (Orquestrador)
│   Orchestrator  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BaseScraper    │  (Interface ABC)
│  - check_availability()
│  - extract()    │
└────────┬────────┘
         │
    ┌────┴────┬────────┬────────┐
    ▼         ▼        ▼        ▼
┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐
│ SC.py │ │ SP.py │ │ PR.py │ │ ...   │
└───────┘ └───────┘ └───────┘ └───────┘
```

---

## 📊 Saída

O pipeline gera um arquivo CSV em `data/output/CUB_COMPILADO_YYYY_MM.csv`:

| Coluna | Descrição |
|--------|-----------|
| Estado | Sigla do estado (SC, SP, etc.) |
| Mes_Referencia | Mês de referência (1-12) |
| Ano_Referencia | Ano de referência |
| Projeto | Tipo de projeto (R8-N, R-16, etc.) |
| Valor | Valor do CUB em R$/m² |
| Unidade | Unidade de medida |
| Data_Extracao | Timestamp da extração |

---

## 🔧 Stack Tecnológico

| Componente | Tecnologia |
|------------|------------|
| Linguagem | Python 3.10+ |
| Navegação | Playwright |
| Parsing HTML | BeautifulSoup4 |
| Parsing PDF | PDFPlumber |
| Validação | Pydantic |
| Dados | Pandas |
| CI/CD | GitHub Actions |

---

## 📄 Licença

Uso interno - MySide.

---

## 👤 Autor

**Mateus Suman Carpenter**

---

> Para documentação técnica detalhada, consulte o arquivo `Documento de Arquitetura CUB Data Pipeline.md`
