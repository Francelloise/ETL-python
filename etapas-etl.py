"""
ETL - Extração, limpeza, transformação e carregamento de dados.
Dados extraídos do CENIPA (Centro de Investigação e Prevenção
de Acidentes Aeronáuticos. Última atualização 18 de Junho de 2015.
""" 

import pandas as pd
import pandera as pa

#1 Etapa: Extração de dados
#separador";" para identificar e separar as colunas, parse_date: converte a coluna em dados do tipo data
df = pd.read_csv("ocorrencia.csv", sep=";", parse_dates['ocorrencia_dia'], dayfirst=true)
df.head(10)

#2 Etapa: Validação de dados
schema = pa.DataFrameSchema(
    columns = {
        "codigo_ocorrencia": pa.Column(pa.Int),
        "codigo_ocorrencia2": pa.Column(pa.Int),
        "ocorrencia_classificacao": pa.Column(pa.String),
        "ocorrencia_cidade": pa.Column(pa.String),
        "ocorrencia_uf": pa.Column(pa.String, pa.Check.str_length(2,2)),
        "ocorrencia_aerodromo": pa.Column(pa.String),
        "ocorrencia_dia": pa.Column(pa.DateTime),
        "ocorrencia_hora": pa.Column(pa.String, pa.Check.str_matches(r'^([0-1]?[0-9]|[2][0-3]):([0-5][0-9])(:[0-5][0-9])?$'), nullable=True),
        "total_recomendacoes": pa.Column(pa.Int)
    }
)

schema.validate(df)

#Tipos de dados extraídos
df.dtypes

#Acessando os meses das datas
df.ocorrencia_dia.dt.month

#Acessando linhas e colunas. Índice do dataframe=labels
df.loc[1, 'ocorrencia_cidade']
df.loc[3]
df.loc[1:3]
df.loc[[10,40]]
df.loc[:, 'ocorrencia_cidade']
#df.iloc[5] pesquisa por valor do índice

#Verificando se são valores únicos para linhas diferentes
df.codigo_ocorrencia.is_unique

#Alterando o código da ocorrência em índice do dataframe
df.set_index('codigo_ocorrencia, inplace=True)
df.loc[40324]

#Removendo o índice e retornando ao índice original
df.reset_index(drop=True, inplace=True)

#Alterando os dados(linha/coluna), forma didática
df.loc[0, 'ocorrencia_aerodromo'] = ''
df.loc[1] = 20
df.loc[:, 'total_recomendacoes'] = 10

#Criando/excluindo coluna de bkp para alteração de dados
df['ocorrencia_uf_bkp'] = df.ocorrencia_uf
df.drop(['ocorrencia_uf_bkp'] axis=1, inplace=True)

#Alteração com base na pesquisa/condição
df.loc[df.ocorrencia_uf == 'SP', ['ocorrencia_classificacao']] = 'GRAVE'
df.loc[df.ocorrencia_uf == 'SP']

"""
3 Etapa: Limpeza de dados
Limpeza dos dados(Dados que não foram informados):
ocorrencia_uf
**

ocorrencia_aerodromo
###!
####
****
*****

ocorrencia_hora
NULL
"""

#df.loc[df.ocorrencia_aerodromo == '****', ['ocorrencia_aerodromo']] = pd.NA
#Para não precisar fazer um a um com esse e os demais utilizar a função replace

df.replace(['**', '###!', '####', '****', '*****', 'NULL'], pd.NA, inplace=True)
df

#Ver os dados não informados do dataframe e somá-los
df.isna().sum()
df.isnull().sum()

#Filtrar valores nulos
#df.ocorrencia_uf.isnull()
#df.loc[df.ocorrencia_uf.isnull()]
filtro = df.ocorrencia_uf.isnull()
df.loc[filtro]

filtro2 = df.ocorrencia_aerodromo.isnull()
df.loc[filtro2]

#Mostra apenas valores informados, não conta os valores nulos
df.count()

#Alternando os dados não informados (inplace=True, executa dentro do dataframe)
#df.fillna(0) apenas em tempo de vizualização
#df.fillna(10, inplace=True) para alterar os dados no arquivo
#df.replace([10], pd.NA, inplace=True)
#df.fillna(value={'total_recomendacoes':10}, inplace=True) alterando apenas os valores de uma coluna
#df.dropna() apaga todas as linhas onde há valores não informados <NA>
#df.dropa(subset=['ocorrencia_uf']) apaga agora as linhas numa coluna específica
#df.drop_duplicates(inplace=True) exclui valores duplicados

#As ocorrências com mais de 10 recomendações
fitro3 = df.total_recomendacoes > 10
df.loc[fitro3]

fitro3 = df.total_recomendacoes > 10
df.loc[fitro3, 'ocorrencia_cidade']

#As ocorrências cuja a classificação == INCIDENTE GRAVE
fitro4 = df.ocorrencia_classificacao == 'INCIDENTE GRAVE'
df.loc[fitro4, 'ocorrencia_cidade']

# 4 Etapa: Transformação

#As ocorrências cuja a classificação == INCIDENTE GRAVE e o estado é SP
fitro4 = df.ocorrencia_classificacao == 'INCIDENTE GRAVE'
fitro5 = df.ocorrencia_uf == 'SP'
df.loc[fitro4 & filtro5]

#As ocorrências cuja a classificação == INCIDENTE GRAVE ou o estado é SP
fitro4 = df.ocorrencia_classificacao == 'INCIDENTE GRAVE'
fitro5 = df.ocorrencia_uf == 'SP'
df.loc[fitro4 | filtro5]

#As ocorrências cuja a (classificação == INCIDENTE GRAVE ou classificação == INCIDENTE)e o estado é SP
fitro4 = (df.ocorrencia_classificacao == 'INCIDENTE GRAVE') | (classificação == 'INCIDENTE') #ou
fitro4 = df.ocorrencia_classificacao,isin(['INCIDENTE GRAVE','INCIDENTE'])
fitro5 = df.ocorrencia_uf == 'SP'
df.loc[fitro4 & filtro5]

#As ocorrências cujas as cidades comecem com a letra C
fitro6 = df.ocorrencia_cidade.str[0] == 'C'
df.loc[fitro6]

#As ocorrências cujas as cidades terminem com a letra A
fitro6 = df.ocorrencia_cidade.str[-1] == 'A'
df.loc[fitro6]

#As ocorrências cujas as cidades terminem com os caracteres MA
fitro6 = df.ocorrencia_cidade.str[-2:] == 'MA'
df.loc[fitro6]

#As ocorrências cujas as cidades que contém (em qualquer parte do conteúdo) os caracteres MA
fitro6 = df.ocorrencia_cidade.str.contains('MA')
df.loc[fitro6]

#As ocorrências cujas as cidades que contém (em qualquer parte do conteúdo) os caracteres MA ou AL
fitro6 = df.ocorrencia_cidade.str.contains('MA|AL')
df.loc[fitro6]

#As ocorrências no ano de 2015
fitro7 = df.ocorrencia_dia.dt.year == 2015
df.loc[fitro7]

#As ocorrências no ano de 2015 e mês 12 (Dezembro)
fitro7 = df.ocorrencia_dia.dt.year == 2015
fitro8 = df.ocorrencia_dia.dt.month == 12
df.loc[fitro7 & fitro8]
#ou
fitro9 = (df.ocorrencia_dia.dt.year) & (df.ocorrencia_dia.dt.month == 12)
df.loc[filtro9]

#As ocorrências no ano de 2015 e mês 12 (Dezembro) e dia 8
fitro7 = df.ocorrencia_dia.dt.year == 2015
fitro8 = df.ocorrencia_dia.dt.month == 12
fitro10 = df.ocorrencia_dia.dt.day == 8
df.loc[fitro7 & fitro8 & fitlro10]

#As ocorrências no ano de 2015 e mês 12 (Dezembro) e entre dias 3 e 8
fitro7 = df.ocorrencia_dia.dt.year == 2015
fitro8 = df.ocorrencia_dia.dt.month == 12
fitro11 = (df.ocorrencia_dia.dt.day > 2) & (df.ocorrencia_dia.dt.day > 9)
df.loc[fitro7 & fitro8 & fitlro11]

#Nova coluna filtrando apenas com os dados que desejo
df['ocorrencia_dia_hora'] = pd.to_datetime(df.ocorrencia_dia.astype(str) + ' ' + df.ocorrencia_hora)

#Agrupamento de dados, um novo dataframe
#As ocorrências no ano de 2015 e mês 3
fitro7 = df.ocorrencia_dia.dt.year == 2015
fitro8 = df.ocorrencia_dia.dt.month == 3
df201503 = df.loc[fitro7 & fitro8]
df201503

df201503.count()

df201503.groupby(['codigo_ocorrencia']).count()
df201503.groupby(['codigo_ocorrencia']).codigo_ocorrencia.count()
df201503.groupby(['ocorrencia_classificacao']).codigo_ocorrencia.count()
df201503.groupby(['ocorrencia_classificacao']).ocorrencia_aerodromo.count()#cuidar quando contar por uma coluna que tem nulo

#Uma solução para isso é usar o size(), agrupa e conta os registros e não os valores
df201503.groupby(['ocorrencia_classificacao']).size()

df201503.groupby(['ocorrencia_classificacao']).size().sort_values()#Ordem rescente
df201503.groupby(['ocorrencia_classificacao']).size().sort_values(ascending=False)#Ordem descrescente

#Agrupamento dos dados da região sudeste no ano de 2010
fitro7 = df.ocorrencia_dia.dt.year == 2010
filtro12 = df.ocorrencia_uf.isin(['SP', 'MG', 'ES', 'RJ'])
dfsudeste2010

#Gerar novos dados através do novos dataframes
dfsudeste2010.groupby(['ocorrencia_classificacao']).size()
dfsudeste2010.groupby(['ocorrencia_classificacao', 'ocorrencia_uf']).size()
dfsudeste2010.groupby(['ocorrencia_uf', 'ocorrencia_classificacao']).size()
dfsudeste2010.groupby(['ocorrencia_cidade']).size(ascending=False)

#Total de recomendações que o Rio de Janeiro teve
filtro13 = dfsudeste2010.ocorrencia_cidade == 'RIO DE JANEIRO'
dfsudeste2010.loc[fltro13].total_recomendacoes.sum()

#Total de recomendações por cidades da região sudeste
dfsudeste2010.groupby(['ocorrencia_cidade']).total_recomendacoes.sum()

#Total de recomendaçõesque por aerodormos, usar o dropna para agrupar os valores não informados
dfsudeste2010.groupby(['ocorrencia_aerodromo'], dropna=False).total_recomendacoes.sum()

#Agrupando por cidade e por mês somando o total de recomendações
filtro14 = dfsudeste2010.total_recomendacoes > 0
dfsudeste2010.loc[filtro14].groupby(['ocorrencia_cidade', dfsudeste2010.ocorrencia_dia.dt.month]).total_recomendacoes.sum