from config.config.settings import get_intraday_url, SOURCE_TABLE, TARGET_TABLE
from extractors.extractors.alpha_vantage import RawDataExtractor, RawDataSource
from processors.processors.data_processor import RawDataProcessor


def main():
    """Função principal que executa o pipeline de dados"""
    print("Iniciando pipeline de dados do AlphaVantage...")

    # 1. Obter a URL da API
    url = get_intraday_url()

    # 2. Extrair dados da API
    with RawDataExtractor() as raw_data_extractor:
        print("Extraindo dados da API...")
        data = raw_data_extractor.get_data(url)

    # 3. Armazenar dados brutos
    with RawDataSource(SOURCE_TABLE) as raw_data_source:
        print("Criando tabela para dados brutos...")
        raw_data_source.create_table()

        print("Inserindo dados brutos...")
        raw_data_source.insert(data)

    # 4. Processar e normalizar dados
    with RawDataProcessor() as raw_data_processor:
        print("Normalizando dados para formato tabular...")
        raw_data_processor.normalize_raw_table(SOURCE_TABLE, TARGET_TABLE)

    print("Pipeline concluído com sucesso!")


if __name__ == '__main__':
    main()
