# ETL-MongoDB


Programa irá fazer ETL dos arquivos ocorrencia.csv, aeronave.csv e fator_contribuinte.csv que possuem as ocorrências de
incidentes e acidentes, tabela de aeronaves e fatores envolvidos nas ocorrencias
investigadas pelo CENIPA e podem ser obtidos no endereço
https://dados.gov.br/dataset/ocorrencias-aeronauticas-da-aviacao-civil-brasileira
e irá gerar um banco de dados MongoDB na nuvem com os resultados, após a limpeza
e tratamento dos dados.
O processo foi construído testando as funções no Jupyter Notebook e após funcionar
criado esse projeto em Python no Pycharm. Desta forma,ganha-se muito tempo para testar.
Obs: das tabelas originais alguns campos não utilizados foram removidos no CSV
