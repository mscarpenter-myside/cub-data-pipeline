# Documento de Arquitetura: CUB Data

# Pipeline

**Versão:** 1.1.0 (Revisão Técnica) **Data:** 13/01/2026 **Responsável:** Mateus Suman Carpenter

## 1. Visão Geral da Solução

O **CUB Data Pipeline** é uma ferramenta de automação focada em processos de ETL
(Extract, Transform, Load) e Monitoramento. O objetivo principal é auditar a disponibilidade
e coletar os dados do Custo Unitário Básico (CUB) nos sites dos Sinduscons estaduais.
Diferente de um scraper tradicional, esta solução implementa uma lógica de **Auditoria de
Disponibilidade** : o sistema verifica proativamente se o dado do mês de referência foi
publicado, gerando alertas distintos para "Sucesso", "Atraso" (dado não disponível na data
esperada) ou "Erro Técnico".

## 2. Estrutura do Projeto (File System)

A solução segue uma estrutura modular para separar responsabilidades e configurações:
cub-scraper-myside/
├── .github/workflows/ # Automação (CI/CD)
├── data/
│ ├── raw/ # Armazenamento temporário (PDFs baixados)
│ └── output/ # Arquivos finais (CSVs formatados)
├── src/
│ ├── core/ # Configurações globais
│ ├── scrapers/ # Lógica específica por estado (Strategy Pattern)
│ │ ├── base.py # Interface (Contrato)
│ │ ├── sc.py # Implementação SC
│ │ └── ...
│ ├── utils/ # Ferramentas (PDF Parser, Notificações)
│ └── main.py # Orquestrador
└── requirements.txt # Dependências

## 3. Padrões de Projeto (Design Patterns)

Para garantir escalabilidade e manutenção, adotamos estritamente:

### 3.1. Strategy Pattern (Extração)

```
● Problema: Heterogeneidade das fontes. SC exige navegação e download de PDF;
SP pode exigir apenas leitura de HTML.
```

```
● Solução: O orquestrador desconhece a lógica interna. Ele interage com a classe
abstrata BaseScraper (herdada de abc.ABC).
● Contrato: Todos os scrapers devem implementar obrigatoriamente dois métodos:
```
1. check_availability(mes, ano): Retorna booleano sem baixar dados pesados.
2. extract(mes, ano): Retorna o objeto validado.

### 3.2. Data Validation (Modelagem)

```
● Implementação: Uso de Pydantic (BaseModel).
● Regra: Não utilizamos dicionários soltos. Os dados extraídos são imediatamente
convertidos em objetos tipados, garantindo que campos numéricos sejam float e
datas sejam respeitadas antes de qualquer processamento.
```
## 4. Stack Tecnológico (Atualizado)

```
Componente Tecnologia Justificativa Técnica
Linguagem Python 3.10+ Padrão de mercado para Engenharia de Dados.
Navegação Playwright Necessário para interagir com sites modernos
(cliques, selects, JS) que o requests não suporta.
Parsing HTML BeautifulSoup4 Eficiência para raspar dados de textos simples e
tabelas HTML.
Parsing PDF PDFPlumber Necessário para extrair dados tabulares de arquivos
PDF (caso comum em SC).
Validação Pydantic Integridade de dados (Type Safety) e serialização.
Persistência Pandas Manipulação e exportação para CSV no layout
MySide.
CI/CD GitHub Actions Execução serverless agendada.
```

## 5. Fluxo de Dados (Data Flow)

1. **Trigger:** O GitHub Actions inicia o fluxo via agendamento (CRON).
2. **Definição de Alvo:** O sistema calcula o "Mês de Referência" esperado (ex: se hoje
    é dia 05/02, buscamos dados de Janeiro).
3. **Auditoria (Check Availability):**
    ○ O Scraper navega até a fonte.
    ○ Verifica a existência de metadados (ex: Texto "Janeiro/2026").
    ○ _Decisão:_ Se não encontrar, interrompe e marca status como **ATRASADO**.
4. **Extração (Se disponível):**
    ○ Realiza o download do arquivo (PDF) ou leitura da tabela (HTML).
    ○ Utiliza Regex/Parsers para limpar o dado bruto.
5. **Validação:** O Pydantic rejeita dados fora do padrão (ex: texto onde deveria ser
    número).
6. **Formatação (CSV Builder):** O dado validado é transformado para o layout exato da
    planilha da MySide.
7. **Notificação:** Envia relatório de status (Sucesso/Atraso/Erro) e o CSV gerado para o
    Slack/ClickUp.

## 6. Glossário Técnico

```
● ABC (Abstract Base Class): Classe que não pode ser instanciada e serve como
modelo obrigatório para outras classes.
● Headless Browser: Navegador web sem interface gráfica (GUI), controlado por
código (Playwright) para simular ações humanas.
● Type Hints: Recurso do Python para indicar explicitamente o tipo de dado esperado
(ex: def func(a: int) -> bool), essencial para o funcionamento do Pydantic.
● Payload: A carga útil de dados. No nosso contexto, refere-se ao objeto JSON ou
CSV final entregue ao usuário.
● Strategy Pattern: Padrão onde algoritmos (scrapers de estados diferentes) são
encapsulados em classes intercambiáveis.
● Audit Check: Etapa de verificação preliminar que distingue "Site fora do ar" de
"Dado ainda não publicado".
● Serialização: Processo de converter um objeto complexo (Classe Python) em um
formato armazenável (JSON/Dict) usando .model_dump().
```

