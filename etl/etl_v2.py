# Exemplo de código simplificado que refatora e agrupa algumas tarefas de ETL
# com base nas sugestões propostas. Ajuste conforme necessário para o seu caso.

import pandas as pd
import numpy as np


def carregar_dados_fonte1(caminho_arquivo: str) -> pd.DataFrame:
    """
    Carrega um DataFrame a partir de uma fonte (ex.: CSV, Excel ou Parquet).
    Ajuste conforme o tipo de arquivo que desejar.
    """
    df = pd.read_csv(caminho_arquivo)  # Exemplo para CSV
    return df


def carregar_dados_fonte2(caminho_arquivo: str) -> pd.DataFrame:
    """
    Outro exemplo de carregamento para outra fonte de dados.
    """
    df = pd.read_csv(caminho_arquivo)  # Ajustar conforme necessidade
    return df


def limpar_colunas(df: pd.DataFrame, colunas_para_remover: list = None) -> pd.DataFrame:
    """
    Remove colunas desnecessárias, renomeia ou realiza outras limpezas
    recorrentes em colunas do DataFrame.
    """
    if colunas_para_remover:
        df = df.drop(columns=colunas_para_remover, errors='ignore')
    # Exemplo de renomeação de colunas:
    # df = df.rename(columns={'coluna_antiga': 'coluna_nova'})
    return df


def remover_duplicados(df: pd.DataFrame, col_chaves: list = None) -> pd.DataFrame:
    """
    Remove duplicados dentro de um DataFrame.
    - col_chaves define quais colunas serão consideradas para identificar duplicados.
    Se col_chaves for None, remove duplicados em todas as colunas.
    """
    if col_chaves:
        df = df.drop_duplicates(subset=col_chaves, keep='first')
    else:
        df = df.drop_duplicates(keep='first')
    return df


def filtrar_valores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica filtros em lote, unificando a lógica para evitar múltiplas passagens no DataFrame.
    Exemplo: remove linhas onde 'coluna1' <= 0 ou 'coluna2' seja nula.
    Ajuste conforme as regras de negócio desejadas.
    """
    condicao = (df['coluna1'] > 0) & (df['coluna2'].notnull())
    df = df[condicao]
    return df


def aplicar_transformacoes_personalizadas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformações específicas (ex.: normalizações,
    converter dados para uma unidade padrão, etc.).
    """
    # Exemplo de vetorização usando NumPy/Pandas:
    # df['coluna_numerica_normalizada'] = np.log(df['coluna_numerica'] + 1)
    return df


def pipeline_etl(caminho_fonte1: str, caminho_fonte2: str) -> pd.DataFrame:
    """
    Função principal que orquestra o processo de ETL,
    agrupando tarefas e chamando as funções de limpeza,
    filtragem e transformações necessárias.
    """
    # 1. Carregar dados
    df1 = carregar_dados_fonte1(caminho_fonte1)
    df2 = carregar_dados_fonte2(caminho_fonte2)

    # 2. Limpar colunas
    df1 = limpar_colunas(df1, colunas_para_remover=['coluna_irrelevante1', 'coluna_irrelevante2'])
    df2 = limpar_colunas(df2, colunas_para_remover=['outra_coluna_irrelevante'])

    # 3. Remover duplicados
    df1 = remover_duplicados(df1, col_chaves=['chave_unica1'])
    df2 = remover_duplicados(df2, col_chaves=['chave_unica2'])

    # 4. Filtrar valores indesejados
    df1 = filtrar_valores(df1)
    df2 = filtrar_valores(df2)

    # 5. Aplicar transformações específicas
    df1 = aplicar_transformacoes_personalizadas(df1)
    df2 = aplicar_transformacoes_personalizadas(df2)

    # 6. Combinar, mesclar ou concatenar, se necessário
    # Exemplo: join pela chave
    df_final = pd.merge(df1, df2, how='inner', left_on='chave_unica1', right_on='chave_unica2')

    return df_final


if __name__ == "__main__":
    # Exemplo de uso:
    caminho_1 = "dados_fonte1.csv"
    caminho_2 = "dados_fonte2.csv"
    df_resultado = pipeline_etl(caminho_1, caminho_2)
    print(df_resultado.head())