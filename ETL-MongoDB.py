#########################################################
# Alessandro Miranda Gonçalves                          #
# Linkedin: www.linkedin.com/alessandromirandagoncalves #
# Março/2022                                            #
#########################################################
# Programa irá fazer ETL do arquivo ocorrencia.csv e aeronave.csv que possuem as ocorrências de
# incidentes e acidentes, tabela de aeronaves e fatores envolvidos nas ocorrencias
# investigadas pelo CENIPA e podem ser obtidos no endereço
# https://dados.gov.br/dataset/ocorrencias-aeronauticas-da-aviacao-civil-brasileira
# e irá gerar um banco de dados MongoDB na nuvem com os resultados.
# O processo foi construído testando as funções no Jupyter Notebook e após funcionar
# criado esse projeto em Python no Pycharm. Desta forma,ganha-se muito tempo para testar.
# Obs: das tabelas originais alguns campos não utilizados foram removidos no CSV

import pandas as pd                 # Biblioteca com funções de ETL
import pandera as pa                # Biblioteca com funções de ETL
import sys                          # Biblioteca com funções de sistema
import datetime                     # para cálculos de tempo usado pelo programa
import pymongo                      # Para exportar dados para MongoDB
from pymongo import MongoClient     # Para exportar dados para MongoDB

def imprimir_cabecalho():  # Exibe informações iniciais do programa
    print(86 * '-')
    print('Programa ETL de arquivo ocorrências aeronáuticas no Brasil com exportação para MongoDB')
    print(86 * '-')


def conectar_banco():  # Conecta ao banco de dados e deixa aconexão aberta em "cliente"
    try:
        print('4. Conectando com MongoDB')
        # Credenciais para conexão
        database_string = "mongodb://localhost:27017"

        cliente = pymongo.MongoClient(database_string)
        print('4.1 Conexão com Mongo com sucesso.')
        conexao = cliente['incidentes_aereos']

    except pymongo.errors.ServerSelectionTimeoutError as e:
        print('*** ERRO: Timeout - Não foi possível conectar ao banco ')
        sys.exit(0)

    return conexao


# Executa a abertura do arquivo ocorrencia.csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_ocor():
    try:
        print('1. Lendo arquivo ocorrencia...')
        valores_ausentes = ['**', '***', '****', '*****', '###!', '####', 'NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_ocor = pd.read_csv("ocorrencia.csv", sep=';', parse_dates=["ocorrencia_dia"], dayfirst=True,
                              na_values=valores_ausentes)
        print('1.1 Arquivo lido com sucesso')
    # Testa se o arquivo existe
    except FileNotFoundError as e:
        print('*** ERRO: Arquivo ocorrencia.csv não encontrado. Favor verificar.')
        sys.exit()
    # Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_ocor


# Executa a abertura do arquivo aeronave.csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_aviao():
    try:
        print('6. Lendo arquivo aeronave...')
        valores_ausentes = ['***', 'NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_aviao = pd.read_csv("aeronave.csv", sep=';', na_values=valores_ausentes)
        print('6.1 Arquivo lido com sucesso')
    # Testa se o arquivo existe
    except FileNotFoundError as e:
        print('*** ERRO: Arquivo aeronave.csv não encontrado. Favor verificar.')
        sys.exit()
    # Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_aviao


# Executa a abertura do arquivo fator_contribuinte.csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_fator():
    try:
        print('9. Lendo arquivo fator_contribuinte...')
        valores_ausentes = ['***', 'NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_fator = pd.read_csv("fator_contribuinte.csv", sep=';', na_values=valores_ausentes)
        print('9.1 Arquivo lido com sucesso')
    # Testa se o arquivo existe
    except FileNotFoundError as e:
        print('*** ERRO: Arquivo fator_contribuinte.csv não encontrado. Favor verificar.')
        sys.exit()
    # Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_fator


# Verifica se o arquivo tem as colunas nos formatos corretos
# Se não, mostra erro e encerra o programa
def validar_arquivo_ocor(df_ocor):
    try:
        print('2. Validando arquivo ocorrencia...')
        schema = pa.DataFrameSchema(
            columns={"codigo_ocorrencia": pa.Column(pa.Int, nullable=True),
                     "codigo_ocorrencia2": pa.Column(pa.Int),
                     "ocorrencia_classificacao": pa.Column(pa.String),
                     "ocorrencia_cidade": pa.Column(pa.String),
                     "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2, 2), nullable=True),
                     "ocorrencia_aerodromo": pa.Column(pa.String, nullable=True),
                     "ocorrencia_dia": pa.Column(pa.DateTime),
                     "ocorrencia_hora": pa.Column(pa.String,
                                                  pa.Check.str_matches(r'^([0-1][0-9]|[2][0-3])(:([0-5][0-9])){1,2}$'),
                                                  nullable=True),
                     "total_recomendacoes": pa.Column(pa.Int)
                     }
        )
        schema.validate(df_ocor, lazy=True)
        print('2.1 Arquivo validado com sucesso')
    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na validação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)  # erros de dataframe ou schema
        print(e.data)  # dataframe inválido
        print(58 * '-')
        sys.exit()


# Verifica se o arquivo tem as colunas nos formatos corretos
# Se não, mostra erro e encerra o programa
def validar_arquivo_aviao(df_aviao):
    try:
        print('7. Validando arquivo aeronave...')
        print('7.1 Excluindo registros duplicados...')
        schema_aviao = pa.DataFrameSchema(
            columns={"codigo_ocorrencia2": pa.Column(pa.Int),
                     "aeronave_matricula": pa.Column(pa.String, nullable=True),
                     "aeronave_operador_categoria": pa.Column(pa.String, nullable=True),
                     "aeronave_tipo_veiculo": pa.Column(pa.String, nullable=True),
                     "aeronave_fabricante": pa.Column(pa.String, nullable=True),
                     "aeronave_modelo": pa.Column(pa.String, nullable=True),
                     "aeronave_tipo_icao": pa.Column(pa.String, nullable=True),
                     "aeronave_motor_tipo": pa.Column(pa.String, nullable=True),
                     "aeronave_motor_quantidade": pa.Column(pa.String, nullable=True),
                     "aeronave_pmd": pa.Column(pa.Int),
                     "aeronave_fatalidades_total": pa.Column(pa.Int)
                     }
        )
        schema_aviao.validate(df_aviao, lazy=True)
        df_aviao.drop_duplicates(['codigo_ocorrencia2'], inplace=True)
        print('7.3 Arquivo validado com sucesso')
    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na validação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)  # erros de dataframe ou schema
        print(e.data)  # dataframe inválido
        print(58 * '-')
        sys.exit()


# Verifica se o arquivo tem as colunas nos formatos corretos
# Se não, mostra erro e encerra o programa
def validar_arquivo_fator(df_fator):
    try:
        print('10. Validando arquivo fator contribuinte...')
        schema_fator = pa.DataFrameSchema(
            columns={"codigo_ocorrencia3": pa.Column(pa.Int),
                     "fator_nome": pa.Column(pa.String, nullable=True),
                     "fator_aspecto": pa.Column(pa.String, nullable=True),
                     "fator_condicionante": pa.Column(pa.String, nullable=True),
                     "fator_area": pa.Column(pa.String, nullable=True)
                     }
        )
        schema_fator.validate(df_fator, lazy=True)
        # registros com codigo_ocorrencia3 podem ser duplicados
        # pois uma ocorrência pode ter divresos fatores contribuintes associados
        print('10.1 Arquivo validado com sucesso')

    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na validação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)  # erros de dataframe ou schema
        print(e.data)  # dataframe inválido
        print(58 * '-')
        sys.exit()


def transformar_arquivo_ocor(df_ocor):
    try:
        # Cria uma nova coluna juntando data com hora e deixando no formato "datetime"
        print('3. Transformando arquivo ocorrencia...')
        df_ocor['ocorrencia_dia_hora'] = pd.to_datetime(
            df_ocor.ocorrencia_dia.astype(str) + ' ' + df_ocor.ocorrencia_hora)
        # Trata para não ocorrer erro de inserção NaT
        df_ocor['ocorrencia_dia_hora'] = df_ocor['ocorrencia_dia_hora'].astype(object).where(
            df_ocor['ocorrencia_dia_hora'].notnull(), None)
        print('3.1 Arquivo transformado com sucesso')

    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na transformação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)  # erros de dataframe ou schema
        print(e.data)  # dataframe inválido
        print(58 * '-')
        sys.exit()


def transformar_arquivo_fator(df_fator):
    try:
        # cria uma coluna Descricao onde vai unir nome,aspecto,condicionante,fator área
        print('11. Transformando arquivo fator...')
        df_fator.rename(columns={'codigo_ocorrencia3': 'codigo_ocorrencia'}, inplace = True)
        df_fator_colunado = df_fator.copy(deep = True)
        df_fator_colunado['fatores'] = df_fator['fator_nome'] + ' ' + df_fator['fator_aspecto'] + ' ' + df_fator[
            'fator_condicionante'] + ' ' + df_fator['fator_area']
        df_fator_colunado.drop(['fator_nome', 'fator_aspecto', 'fator_condicionante'], axis=1, inplace=True)
        df_fator_colunado.drop(['fator_area'], axis=1, inplace=True)
        df_fator_colunado = df_fator_colunado[['codigo_ocorrencia', 'fatores']]
        # Agrupa para ficar somente com colunas: codigo-ocorrencia e descricao
        df_fator_agrupado = df_fator_colunado.groupby('codigo_ocorrencia')
        #Executa uma função para transformar de DataFrameGroup para DataFrame e poder unir com outros arquivos
        df_fator_agrupado = df_fator_agrupado.sum().reset_index()
        df_fator_agrupado.set_index("codigo_ocorrencia")
        print('11.1 Arquivo transformado com sucesso')

    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na transformação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)  # erros de dataframe ou schema
        print(e.data)  # dataframe inválido
        print(58 * '-')
        sys.exit()
    return df_fator_agrupado

# Junta os dataframes para gerar somente um dataframe para ser exportado para o Mongodb
def unir_arquivos(df_ocor, df_aviao, df_fator):  # , df_fator):
    #Primeiramente une ocorrencia com aeronave
    r = pd.merge(df_ocor, df_aviao, how='left', on='codigo_ocorrencia2')
    #Agora une o resultado da união acima com fator
    r_novo= pd.merge(r, df_fator, how='left', on='codigo_ocorrencia')
    return r_novo

# Copia o Dataframe final para o Mongodb na colecao historico
def exportar_Mongo(dbname, r):
    colecao = "historico"
    print("12 Exportando para o MongoDb - banco: ", dbname.name)
    r.reset_index(inplace=True)
    r_dicionario = r.to_dict("records")
    print("12.1 Preparando registros na coleção: ", colecao)
    collection_name = dbname[colecao]
    #Apaga a coleção para inserir todos os registros
    dbname.dropDatabase
    collection_name.insert_many(r_dicionario)
    print("13. Dados exportados com sucesso. Registros: ",r.shape[0])

if __name__ == "__main__":
    tempo_inicial = datetime.datetime.now()
    imprimir_cabecalho()
    dbname = conectar_banco()

    # Fazer ETL com ocorrências primeiramente
    df_ocor = abrir_arquivo_ocor()
    validar_arquivo_ocor(df_ocor)
    transformar_arquivo_ocor(df_ocor)
    cliente = conectar_banco()

    # Fazer ETL com aeronaves
    df_aviao = abrir_arquivo_aviao()
    validar_arquivo_aviao(df_aviao)
    ## Não existem transformações a serem feitas em aeronaves

    # Fazer ETL com fator contribuinte
    df_fator = abrir_arquivo_fator()
    validar_arquivo_fator(df_fator)
    df_fator = transformar_arquivo_fator(df_fator)

    r = unir_arquivos(df_ocor, df_aviao, df_fator)
    exportar_Mongo(dbname, r)

    tempo_final = datetime.datetime.now()
    tempo_total = tempo_final - tempo_inicial
    print("\nTempo total transcorrido (em s): {}".format(tempo_total))