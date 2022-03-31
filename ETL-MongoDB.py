#########################################################
# Alessandro Miranda Gonçalves                          #
# Linkedin: www.linkedin.com/alessandromirandagoncalves #
# Março/2022                                            #
#########################################################
# Programa irá fazer ETL do arquivo ocorrencia.csv e aeronave.csv que possuem as ocorrências de
# incidentes e acidentes, tabela de aeronaves e aeródromos envolvidos nas ocorrencias
# investigadas pelo CENIPA e podem ser obtidos no endereço
# https://dados.gov.br/dataset/ocorrencias-aeronauticas-da-aviacao-civil-brasileira
# e irá gerar um banco de dados MongoDB na nuvem com os resultados.
# O processo foi construído testando as funções no Jupyter Notebook e após funcionar
# criado esse projeto em Python no Pycharm. Desta forma,ganha-se muito tempo para testar.
# Obs: das tabelas originais alguns campos não utilizados foram removidos no CSV

import pandas as pd         # Biblioteca com funções de ETL
import pandera as pa        # Biblioteca com funções de ETL
import sys                  # Biblioteca com funções de sistema
import sqlalchemy as sql    # Permite manipulação de dados Mysql
import datetime             # para cálculos de tempo usado pelo programa


def imprimir_cabecalho():  # Exibe informações iniciais do programa
    print(58*'-')
    print('Programa ETL de arquivo ocorrências aeronáuticas no Brasil')
    print(58*'-')


def conectar_banco():  # Conecta ao banco de dados e deixa aconexão aberta em "conexao"
    try:
        print('4. Conectando com Mysql')
        # Credenciais para conexão Mysql
        database_username = 'teste'
        database_password = 'teste'
        database_ip = '127.0.0.1'
        database_name = 'mysql'

        # Primeiramente se conecta ao Mysql para poder criar o banco CENIPA
        conexao = sql.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                    format(database_username, database_password,
                                    database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
        print('4.1 Conexão com Mysql com sucesso.')
        criar_banco_mysql(conexao)
        # Primeiramente se conecta ao Mysql para poder criar o banco CENIPA
        database_name = 'cenipa'
        conexao = sql.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                   format(database_username, database_password,
                                          database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()
        print('4.4 Conexão com CENIPA com sucesso')
    except sql.exc.DatabaseError as e:
        print('*** ERRO: Não foi possível conectar ao banco {} no servidor {} porta:3306.'.format(database_name,database_ip))
        sys.exit(0)
    return conexao

# Executa a abertura do arquivo ocorrencia.csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_ocor():
    try:
        print('1. Lendo arquivo ocorrencia...')
        valores_ausentes=['**','***','****','*****','###!','####','NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_ocor = pd.read_csv("ocorrencia.csv",sep=';',parse_dates=["ocorrencia_dia"],dayfirst=True,na_values=valores_ausentes)
        print('1.1 Arquivo lido com sucesso')
    #Testa se o arquivo existe
    except FileNotFoundError as e:
         print('*** ERRO: Arquivo ocorrencia.csv não encontrado. Favor verificar.')
         sys.exit()
    #Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_ocor

# Executa a abertura do arquivo aeronave.csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_aviao():
    try:
        print('6. Lendo arquivo aeronave...')
        valores_ausentes=['***','NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_aviao = pd.read_csv("aeronave.csv",sep=';',na_values=valores_ausentes)
        print('6.1 Arquivo lido com sucesso')
    #Testa se o arquivo existe
    except FileNotFoundError as e:
         print('*** ERRO: Arquivo aeronave.csv não encontrado. Favor verificar.')
         sys.exit()
    #Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_aviao

# Executa a abertura do arquivo aerodromo.csv e coloca NA e NAN nos valores não informados
# a fim de facilitar a importação
def abrir_arquivo_aero():
    try:
        print('9. Lendo arquivo aerodromo...')
        valores_ausentes=['***','NULL']
        # Ao encontrar algo especificado em "valores_ausentes", estes serão automaticamente convertidos para Na ou Nan
        df_aviao = pd.read_csv("aeronave.csv",sep=';',na_values=valores_ausentes)
        print('9.1 Arquivo lido com sucesso')
    #Testa se o arquivo existe
    except FileNotFoundError as e:
         print('*** ERRO: Arquivo aerodromo.csv não encontrado. Favor verificar.')
         sys.exit()
    #Outros erros são exibidos
    except BaseException as e:
        print("*** ERRO: ".format(e))
        sys.exit()
    return df_aero

# Verifica se o arquivo tem as colunas nos formatos corretos
# Se não, mostra erro e encerra o programa
def validar_arquivo_ocor(df_ocor):
    try:
        print('2. Validando arquivo ocorrencia...')
        schema = pa.DataFrameSchema(
            columns={"codigo_ocorrencia": pa.Column(pa.Int,nullable=True),
                     "codigo_ocorrencia2": pa.Column(pa.Int),
                     "ocorrencia_classificacao": pa.Column(pa.String),
                     "ocorrencia_cidade": pa.Column(pa.String),
                     "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2, 2),nullable=True),
                     "ocorrencia_aerodromo": pa.Column(pa.String, nullable=True),
                     "ocorrencia_dia": pa.Column(pa.DateTime),
                     "ocorrencia_hora": pa.Column(pa.String,
                                                  pa.Check.str_matches(r'^([0-1][0-9]|[2][0-3])(:([0-5][0-9])){1,2}$'),
                                                  nullable=True),
                     "total_recomendacoes": pa.Column(pa.Int)
                     }
        )
        schema.validate(df_ocor,lazy=True)
        print('2.1 Arquivo validado com sucesso')
    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na validação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)    # erros de dataframe ou schema
        print(e.data)             # dataframe inválido
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
        schema_aero.validate(df_aviao,lazy=True)
        df_aviao.drop_duplicates(['codigo_ocorrencia2'],inplace=True)
        print('7.3 Arquivo validado com sucesso')
    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na validação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)    # erros de dataframe ou schema
        print(e.data)             # dataframe inválido
        print(58 * '-')
        sys.exit()

def transformar_arquivo_ocor(df_ocor):
    try:
        #Cria uma nova coluna juntando data com hora e deixando no formato "datetime"
        print('3. Transformando arquivo ocorrencia...')
        df_ocor['ocorrencia_dia_hora'] = pd.to_datetime(df_ocor.ocorrencia_dia.astype(str) + ' ' + df_ocor.ocorrencia_hora)
        print('3.1 Arquivo transformado com sucesso')

    except pa.errors.SchemaErrors as e:
        print('*** Erros encontrados na transformação. Favor verificar:')
        print(58 * '-')
        print(e.failure_cases)    # erros de dataframe ou schema
        print(e.data)             # dataframe inválido
        print(58 * '-')
        sys.exit()

#cria banco Mysql e exporta dados do dataframe para a tabela "ocorrencias"
def criar_banco_mysql(conexao):
    try:
        print('4.2 Criando banco Mysql...')
        conexao.execute('create database CENIPA;')
        print('4.3 Banco Mysql gerado.')
    # Banco já existe. Despreza erro de criação
    except sql.exc.DatabaseError as e:
        print('>>> Aviso: Banco já existe. Continuando. <<<')
    except ValueError as e:
        print('***ERRO: '.format(e))
        pass

#exporta dados do dataframe para a tabela "ocorrencias"
def exportar_mysql_ocor(conexao,df_ocor):
    try:
        print('5 Gerando tabela ocorrencias...')
        # Convert dataframe to sql table
        df_ocor.to_sql('ocorrencias', conexao, index=False)
    # Tabela já existe. Despreza erro de criação e continua.
    except ValueError as e:
        print('>>> Aviso: Tabela já existe. Continuando. <<<')

    try:
        print('5.1 Adicionando chave primária...')
        conexao.execute('ALTER TABLE ocorrencias ADD PRIMARY KEY (`codigo_ocorrencia`);')
    except sql.exc.ProgrammingError as e:
        print('>>> Aviso: chave primária já existe. Continuando... <<<')

    try:
        print('5.2 Adicionando índice para chaves estrangeiras...')
        conexao.execute('create index idx_codigo_ocorrencia2 on ocorrencias(codigo_ocorrencia2);')
    except sql.exc.ProgrammingError as e:
        print('>>> Aviso: índice já existe. Continuando... <<<')
    print('5.3 Tabela ocorrencias gerada.')

#Abre banco Mysql e exporta dados do dataframe para a tabela "aeronaves"
def exportar_mysql_aviao(conexao,df_aviao):
    try:
        # Converte dataframe para a tabela Mysql
        print('8 Gerando tabela aeronaves...')
        df_aviao.to_sql('aeronaves', conexao, index=False)

    except ValueError as e:
        print('>>> Aviso: Tabela já existe. Continuando. <<<')

# SQL muito grande
    except sql.exc.OperationalError as e:
        print('ERRO: Got a packet bigger than `max_allowed_packet` bytes')
        print('Execute as linhas abaixo no seu ambiente Mysql:')
        print('   set global net_buffer_length=1000000;')
        print('   set global max_allowed_packet=1000000000;')
        sys.exit(0)

    try:
        print('8.1 Adicionando chave primária...')
        conexao.execute('ALTER TABLE aeronaves ADD PRIMARY KEY (`codigo_ocorrencia2`);')
    except sql.exc.ProgrammingError as e:
        print('>>> Aviso: chave primária já existe. Continuando... <<<')

    try:
        print('8.2 Adicionando chave estrangeira...')
        conexao.execute('ALTER TABLE ocorrencias ADD CONSTRAINT FK_OCORRENCIA_2 '
                        'FOREIGN KEY (`codigo_ocorrencia2`) '
                        'REFERENCES aeronaves(`codigo_ocorrencia2`);')
    except sql.exc.DatabaseError as e:
        print('>>> Aviso: chave estrangeira já existe. Continuando... <<<')

    print('8.3 Banco Mysql alterado.')

if __name__ == "__main__":
    tempo_inicial = datetime.datetime.now()
    imprimir_cabecalho()

    # Fazer ETL com ocorrências primeiramente
    df_ocor = abrir_arquivo_ocor()
    validar_arquivo_ocor(df_ocor)
    transformar_arquivo_ocor(df_ocor)
    conexao = conectar_banco()
    exportar_mysql_ocor(conexao,df_ocor)

    # Fazer ETL com aeronaves
    df_aero = abrir_arquivo_aero()
    validar_arquivo_aero(df_aero)
    ## Não existem transformações a serem feitas em aeronaves por isso passará à exportação
    exportar_mysql_aero(conexao,df_aero)

    # Fazer ETL com aeródromos
    df_aero = abrir_arquivo_aero()
    validar_arquivo_aero(df_aero)
    ## Não existem transformações a serem feitas em aeronaves por isso passará à exportação
    exportar_mysql_aero(conexao,df_aero)

    tempo_final = datetime.datetime.now()
    tempo_total = tempo_final-tempo_inicial
    print("\nTempo total transcorrido (em s): {}".format(tempo_total))