# import pandas as pd
# from supabase import create_client, Client
# import os
# from dotenv import load_dotenv
# import numpy as np
# from typing import Union, List # Importado para tipagem

# # ========== CONFIGURAÇÃO INICIAL ==========
# load_dotenv()
# SUPABASE_URL = os.getenv("SUPABASE_URL")
# SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# def normalize_column_name(column_name: str) -> str:
#     """Normaliza o nome da coluna para remover caracteres especiais e acentos."""
#     import unicodedata
#     normalized = unicodedata.normalize('NFKD', column_name).encode('ascii', 'ignore').decode('utf-8')
#     # Aumentado o escopo de substituição de caracteres para evitar problemas de nome
#     normalized = normalized.replace(" ", "_").replace("/", "_").replace(".", "_").replace("(", "").replace(")", "").replace("-", "_").lower()
#     return normalized

# def add_table_supabase(sheet_url: str, table_name: str, primary_key_column: Union[str, List[str]], columns_int: int):
#     """
#     Sincroniza dados de uma planilha do Google Sheets com uma tabela do Supabase,
#     suportando chaves primárias simples ou compostas.
#     """
#     if not sheet_url:
#         print(f"❌ URL para a tabela '{table_name}' não encontrada no arquivo .env")
#         return

#     # 🌟 PASSO 1: TRATAMENTO E NORMALIZAÇÃO DA CHAVE PRIMÁRIA 🌟
#     if isinstance(primary_key_column, list):
#         primary_key_cols_lower = [normalize_column_name(col) for col in primary_key_column]
#         primary_key_str_sql = ", ".join(primary_key_cols_lower) # Para o on_conflict do Supabase (SQL)
#     else:
#         primary_key_cols_lower = [normalize_column_name(primary_key_column)]
#         primary_key_str_sql = primary_key_cols_lower[0]
        
#     primary_key_name_display = ", ".join(primary_key_cols_lower) # Para mensagens de console

#     # ========== EXTRAÇÃO & PREPARAÇÃO DOS DADOS ==========
#     try:
#         df = pd.read_csv(sheet_url)
#     except Exception as e:
#         print(f"❌ Erro ao ler o arquivo CSV da URL para {table_name}: {e}")
#         return

#     # Seleciona e normaliza colunas
#     df = df.iloc[:, :columns_int]
#     df.columns = [normalize_column_name(c) for c in df.columns]
    
#     # 💡 DEBUG: Verificação para resolver o KeyError
#     if not all(col in df.columns for col in primary_key_cols_lower):
#         print(f"❌ ERRO GRAVE DE COLUNA em '{table_name}'!")
#         print(f"Keys esperadas no DataFrame: {primary_key_cols_lower}")
#         print(f"Keys reais do DataFrame: {df.columns.tolist()}")
#         print("Ajuste os nomes em 'primary_key' no seu JSON para corresponderem às colunas da planilha após a normalização.")
#         return # Para a execução se o erro for de coluna

#     # Preenche valores vazios com NaN
#     df.replace("", np.nan, inplace=True)
    
#     # Remove linhas totalmente vazias
#     df.dropna(how="all", inplace=True)
    
#     # 🌟 PASSO 2: USAR LISTA DE CHAVES NO DROPNA 🌟
#     df.dropna(subset=primary_key_cols_lower, inplace=True)
    
#     # Substitui NaN por None para o Supabase
#     df = df.where(pd.notnull(df), None)

#     # ========== CONEXÃO ==========
#     try:
#         supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
#     except Exception as e:
#         print(f"❌ Erro ao conectar ao Supabase: {e}")
#         return

#     # ========== SINCRONIZAÇÃO ==========
#     try:
#         # A lógica de delete+insert foi simplificada, assumindo que você usará o upsert se a chave existir
#         # A verificação de existência da coluna na planilha foi movida para o bloco de DEBUG acima

#         print(f"🔄 Sincronizando tabela '{table_name}' usando 'upsert' com '{primary_key_name_display}'.")

#         # Se houver uma chave composta, a lógica de DELETE de linhas inexistentes no Sheet fica complexa e foi removida para focar no UPSERT.

#         for row in df.to_dict(orient="records"):
#             clean_row = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            
#             # 🌟 PASSO 3: USAR CHAVE COMPOSTA NO UPSERT 🌟
#             supabase.table(table_name).upsert(
#                 clean_row, 
#                 on_conflict=primary_key_str_sql
#             ).execute()

#         print(f"✅ Sincronização completa para a tabela '{table_name}'!")

#     except Exception as e:
#         print(f"❌ Erro durante a sincronização da tabela {table_name}")
#         print(e)
# # ========== LISTA DE TABELAS PARA SINCRONIZAR ==========
# tables_config = [
#     {
#         "sheet_url": os.getenv("CADASTRO_SPREADSHEET"), 
#         "table_name": "cadastro", 
#         "primary_key": "PROCESSO", 
#         "columns_count": 8
#     },
#     {
#         "sheet_url": os.getenv("CONVENIO_SPREADSHEET"), 
#         "table_name": "convenio", 
#         "primary_key": "PROCESSO", 
#         "columns_count": 10
#     },
#     {
#         "sheet_url": os.getenv("GEPRO_SPREADSHEET"), 
#         "table_name": "gepro", 
#         "primary_key": "PROCESSO", 
#         "columns_count": 17
#     },
#     {
#         "sheet_url": os.getenv("GEPRE_SPREADSHEET"), 
#         "table_name": "gepre", 
#         "primary_key": "PROCESSO", 
#         "columns_count": 15
#     },
#     {
#         "sheet_url": os.getenv("COEDI_SPREADSHEET"), 
#         "table_name": "coedi", 
#         "primary_key": "PROCESSO", 
#         "columns_count": 19
#     },
#     {
#         "sheet_url": os.getenv("MODALIDADE_SPREADSHEET"), 
#         "table_name": "modalidade", 
#         "primary_key": "PROCESSO", 
#         "columns_count": 23
#     },
#     {
#         "sheet_url": os.getenv("FLUXO_2_0_SPREADSHEET"), 
#         "table_name": "fluxo_2_0", 
#         "primary_key": "PROCESSO",
#         "columns_count": 16
#     },
#     {
#        "sheet_url": os.getenv("DOM_SPREADSHEET"), 
#        "table_name": "publicacoes", 
#        "primary_key": ["PROCESSO","DATA_DO_DOM","TIPO","MODALIDADE"],
#        "columns_count": 13
#     },

# ]

# # ========== EXECUTA A SINCRONIZAÇÃO PARA TODAS AS TABELAS ==========
# if not SUPABASE_URL or not SUPABASE_KEY:
#     print("❌ Por favor, configure as variáveis SUPABASE_URL e SUPABASE_KEY no arquivo .env")
# else:
#     for table_info in tables_config:
#         add_table_supabase(
#             sheet_url=table_info["sheet_url"],
#             table_name=table_info["table_name"],
#             primary_key_column=table_info["primary_key"],
#             columns_int=table_info["columns_count"]
#         )


import pandas as pd
from supabase import create_client, Client
import os
# Removido: from dotenv import load_dotenv  <-- Remover esta importação
import numpy as np
from typing import Union, List 

# ========== CONFIGURAÇÃO INICIAL ==========
# Removido: load_dotenv()  <-- Remover esta chamada
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def normalize_column_name(column_name: str) -> str:
    """Normaliza o nome da coluna para remover caracteres especiais e acentos."""
    import unicodedata
    normalized = unicodedata.normalize('NFKD', column_name).encode('ascii', 'ignore').decode('utf-8')
    # Aumentado o escopo de substituição de caracteres para evitar problemas de nome
    normalized = normalized.replace(" ", "_").replace("/", "_").replace(".", "_").replace("(", "").replace(")", "").replace("-", "_").lower()
    return normalized

def add_table_supabase(sheet_url: str, table_name: str, primary_key_column: Union[str, List[str]], columns_int: int):
    """
    Sincroniza dados de uma planilha do Google Sheets com uma tabela do Supabase,
    suportando chaves primárias simples ou compostas.
    """
    if not sheet_url:
        print(f"❌ URL para a tabela '{table_name}' não encontrada no ambiente do GitHub Actions.")
        return

    # 🌟 PASSO 1: TRATAMENTO E NORMALIZAÇÃO DA CHAVE PRIMÁRIA 🌟
    if isinstance(primary_key_column, list):
        primary_key_cols_lower = [normalize_column_name(col) for col in primary_key_column]
        primary_key_str_sql = ", ".join(primary_key_cols_lower) # Para o on_conflict do Supabase (SQL)
    else:
        primary_key_cols_lower = [normalize_column_name(primary_key_column)]
        primary_key_str_sql = primary_key_cols_lower[0]
        
    primary_key_name_display = ", ".join(primary_key_cols_lower) # Para mensagens de console

    # ========== EXTRAÇÃO & PREPARAÇÃO DOS DADOS ==========
    try:
        df = pd.read_csv(sheet_url)
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo CSV da URL para {table_name}: {e}")
        return

    # Seleciona e normaliza colunas
    df = df.iloc[:, :columns_int]
    df.columns = [normalize_column_name(c) for c in df.columns]
    
    # 💡 DEBUG: Verificação para resolver o KeyError
    if not all(col in df.columns for col in primary_key_cols_lower):
        print(f"❌ ERRO GRAVE DE COLUNA em '{table_name}'!")
        print(f"Keys esperadas no DataFrame: {primary_key_cols_lower}")
        print(f"Keys reais do DataFrame: {df.columns.tolist()}")
        print("Ajuste os nomes em 'primary_key' no seu JSON para corresponderem às colunas da planilha após a normalização.")
        return # Para a execução se o erro for de coluna

    # Preenche valores vazios com NaN
    df.replace("", np.nan, inplace=True)
    
    # Remove linhas totalmente vazias
    df.dropna(how="all", inplace=True)
    
    # 🌟 PASSO 2: USAR LISTA DE CHAVES NO DROPNA 🌟
    df.dropna(subset=primary_key_cols_lower, inplace=True)
    
    # Substitui NaN por None para o Supabase
    df = df.where(pd.notnull(df), None)

    # ========== CONEXÃO ==========
    try:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as e:
        print(f"❌ Erro ao conectar ao Supabase: {e}")
        return

    # ========== SINCRONIZAÇÃO ==========
    try:
        # A lógica de delete+insert foi simplificada, assumindo que você usará o upsert se a chave existir
        # A verificação de existência da coluna na planilha foi movida para o bloco de DEBUG acima

        print(f"🔄 Sincronizando tabela '{table_name}' usando 'upsert' com '{primary_key_name_display}'.")

        # Se houver uma chave composta, a lógica de DELETE de linhas inexistentes no Sheet fica complexa e foi removida para focar no UPSERT.

        for row in df.to_dict(orient="records"):
            clean_row = {k: (None if pd.isna(v) else v) for k, v in row.items()}
            
            # 🌟 PASSO 3: USAR CHAVE COMPOSTA NO UPSERT 🌟
            supabase.table(table_name).upsert(
                clean_row, 
                on_conflict=primary_key_str_sql
            ).execute()

        print(f"✅ Sincronização completa para a tabela '{table_name}'!")

    except Exception as e:
        print(f"❌ Erro durante a sincronização da tabela {table_name}")
        print(e)
# ========== LISTA DE TABELAS PARA SINCRONIZAR ==========
tables_config = [
    {
        "sheet_url": os.getenv("CADASTRO_SPREADSHEET"), 
        "table_name": "cadastro", 
        "primary_key": "PROCESSO", 
        "columns_count": 8
    },
    {
        "sheet_url": os.getenv("CONVENIO_SPREADSHEET"), 
        "table_name": "convenio", 
        "primary_key": "PROCESSO", 
        "columns_count": 10
    },
    {
        "sheet_url": os.getenv("GEPRO_SPREADSHEET"), 
        "table_name": "gepro", 
        "primary_key": "PROCESSO", 
        "columns_count": 17
    },
    {
        "sheet_url": os.getenv("GEPRE_SPREADSHEET"), 
        "table_name": "gepre", 
        "primary_key": "PROCESSO", 
        "columns_count": 15
    },
    {
        "sheet_url": os.getenv("COEDI_SPREADSHEET"), 
        "table_name": "coedi", 
        "primary_key": "PROCESSO", 
        "columns_count": 19
    },
    {
        "sheet_url": os.getenv("MODALIDADE_SPREADSHEET"), 
        "table_name": "modalidade", 
        "primary_key": "PROCESSO", 
        "columns_count": 23
    },
    {
        "sheet_url": os.getenv("FLUXO_2_0_SPREADSHEET"), 
        "table_name": "fluxo_2_0", 
        "primary_key": "PROCESSO",
        "columns_count": 16
    },
    {
       "sheet_url": os.getenv("DOM_SPREADSHEET"), 
       "table_name": "publicacoes", 
       "primary_key": ["PROCESSO","DATA_DO_DOM","TIPO","MODALIDADE"],
       "columns_count": 13
    },

]

# ========== EXECUTA A SINCRONIZAÇÃO PARA TODAS AS TABELAS ==========
if not SUPABASE_URL or not SUPABASE_KEY:
    # A mensagem de erro foi adaptada para o contexto do GitHub Actions.
    print("❌ Falha na injeção de secrets. Certifique-se de que SUPABASE_URL e SUPABASE_KEY estão configuradas corretamente nas Secrets do GitHub.")
else:
    for table_info in tables_config:
        add_table_supabase(
            sheet_url=table_info["sheet_url"],
            table_name=table_info["table_name"],
            primary_key_column=table_info["primary_key"],
            columns_int=table_info["columns_count"]
        )