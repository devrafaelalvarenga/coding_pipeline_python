
alphavantage-data-pipeline/

### README.md

# AlphaVantage Data Pipeline

Um pipeline de dados simples para extrair, armazenar e processar dados de séries temporais da API Alpha Vantage.

## Descrição

Este projeto extrai dados de séries temporais intradia da API Alpha Vantage, armazena os dados brutos em uma tabela SQLite e os processa em um formato tabular padronizado para análise.

## Requisitos

- Python 3.12+
- pandas
- requests

## Instalação

1. Clone o repositório:
   ```
   git clone https://github.com/xxxx/alphavantage-data-pipeline.git
   cd alphavantage-data-pipeline
   ```

2. Configure sua chave de API Alpha Vantage em `config/settings.py`.

## Uso

Execute o pipeline completo:

```
python main.py
```

## Estrutura do Projeto

- `src/`: Contém todos os módulos do código fonte
  - `db/`: Módulos de conexão ao banco de dados
  - `extractors/`: Módulos para extrair dados da API Alpha Vantage
  - `processors/`: Módulos para processar e transformar dados
- `config/`: Configurações do projeto
- `main.py`: Script principal

## Licença

```

## Proximos passos?

1. **Documentar código:** Adicionar docstrings a todas as classes e funções
2. **Testes automatizados:** Criar uma pasta `tests/` com testes unitários
3. **Logs estruturados:** Implementar um sistema de logging em vez de usar print