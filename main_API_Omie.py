import requests
import json
import pandas as pd
# import numpy as np
import sys
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import os
import datetime
from datetime import date,timedelta
import chardet


load_dotenv()

#################Variáveis###########################

app_key_dustre = os.getenv('app_key_dustre')
app_secret_dustre = os.getenv('app_secret_dustre')
familia_dustre = os.getenv('familia_dustre')

app_key_forge = os.getenv('app_key_forge')
app_secret_forge = os.getenv('app_secret_forge')

app_key_ES = os.getenv('app_key_ES')
app_secret_ES = os.getenv('app_secret_ES')
familia_ES = os.getenv('familia_ES')

##################################################################

print('Iniciando Atualização em 6 etapas')

inicio = datetime.datetime.now()
print('Inicio: ''{}'.format(inicio))

print('Bases Movimentos Omie. Etapa 1 de 6')

# movimentos Omie
    
url = "https://app.omie.com.br/api/v1/financas/mf/"


###### Trunca a tabela stg_movimentos_omie

conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

conn.autocommit = True

cur = conn.cursor()
cur.execute(f"DROP TABLE IF EXISTS STAGE.STG_MOVIMENTOS_OMIE_DUSTRE")
cur.execute(f"DROP TABLE IF EXISTS STAGE.STG_MOVIMENTOS_OMIE_FORGE")
cur.execute(f"DROP TABLE IF EXISTS STAGE.STG_MOVIMENTOS_OMIE_ES")
conn.commit()
cur.close()
conn.close()


###### Processo alimenta os dados de movimentos
empresas = ['Dustre','Forge','ES']
for empresa in empresas:
    ep = empresa
    if ep == 'Dustre':
        app_key = app_key_dustre
        app_secret = app_secret_dustre
    elif ep == 'Forge':
        app_key = app_key_forge
        app_secret = app_secret_forge
    else:
        app_key = app_key_ES
        app_secret = app_secret_ES

    payload = json.dumps({
    "call": "ListarMovimentos",
    "app_key": app_key,
    "app_secret": app_secret,
    "param": [
        {
        "nPagina": 1,
        "nRegPorPagina": 500
        # "lDadosCad":"S",
        # "dDtAltDe": "01/11/2022",
        # "dDtAltAte":"31/12/2050"
        }
    ]
    })
    headers = { 
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    result =  response.json()

    totalPaginas = result['nTotPaginas']
    totalRegistros = result['nTotRegistros']
    print('>>>>>> Processo movimentos: ''{}'.format(ep) +' <<<<<<<')
    print('Total de Paginas: ''{}'.format(totalPaginas))
    print('Total de Registros: ''{}'.format(totalRegistros))

    consulta_results = []
    
    for pageNum in range(1,int(totalPaginas)+1):
        url = "https://app.omie.com.br/api/v1/financas/mf/"
        payload = json.dumps({
        "call": "ListarMovimentos",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [
            {
            "nPagina": '{}'.format(pageNum),
            "nRegPorPagina": 500
            # "lDadosCad":"S",
            # "dDtAltDe": "01/11/2022",
            # "dDtAltAte":"31/12/2050"
            
            }
        ]
        })
        headers = { 
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        result = response.json()

        consulta_results = consulta_results + [result]

        resp = consulta_results
        print('Extraindo Página: ''{}'.format(pageNum) + ' de ''{}'.format(totalPaginas))

        df = pd.json_normalize(resp, record_path=['movimentos'],meta_prefix=False)

        df = df.assign(Origem=ep)
        tab = ep.lower()
        
    # Conecta no Postgre e alimenta a Stage movimentos
    engine=create_engine("postgresql+psycopg2://postgres:""{}".format(os.getenv('password')) + "@""{}".format(os.getenv('host')) + ":5432/postgres")

    df.to_sql(name='stg_movimentos_omie_''{}'.format(tab), con=engine, if_exists='append',index=False,schema='stage')

fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio))


# Alimenta Processo Movimento Estoque

print('Bases Movimento Estoque Omie. Etapa 2 de 6')

inicioEstoque = datetime.datetime.now()

url = "https://app.omie.com.br/api/v1/estoque/consulta/"

    
    ###### Trunca a tabela stg_movimentos_omie

conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

conn.autocommit = True

cur = conn.cursor()
cur.execute(f"TRUNCATE TABLE STAGE.STG_MOVIMENTO_ESTOQUE")
conn.commit()
cur.close()
conn.close()



data_atual = date.today() + timedelta(days=-40)
hoje = '{}/{}/{}'.format(data_atual.day, data_atual.month,data_atual.year)
print('Extraindo dados a partir de: ''{}'.format(hoje))

empresas = ['Dustre','Forge','ES']
for empresa in empresas:
    ep = empresa
    if ep == 'Dustre':
        app_key = app_key_dustre
        app_secret = app_secret_dustre
    elif ep == 'Forge':
        app_key = app_key_forge
        app_secret = app_secret_forge
    else:
        app_key = app_key_ES
        app_secret = app_secret_ES

    payload = json.dumps({
    "call": "ListarMovimentoEstoque",
    "app_key": app_key,
    "app_secret": app_secret,
    "param": [
        {
        "nPagina": 1,
        "nRegPorPagina": 100,
        "codigo_local_estoque":0,
        "idProd":0,
        "lista_local_estoque":"",
        "dDtInicial": '{}'.format(hoje),
        "dDtFinal": "31/12/2050"
        
        }
    ]
    })
    headers = { 
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    result =  response.json()

    # print(result)

    totalPaginas = result['nTotPaginas']
    totalRegistros = result['nTotRegistros']
    print('>>>>>> Processo Movimento Estoque: ''{}'.format(ep) +' <<<<<<<')
    print('Total de Paginas: ''{}'.format(totalPaginas))
    print('Total de Registros: ''{}'.format(totalRegistros))

    consulta_results = []

    for pageNum in range(1,int(totalPaginas)+1):
        url = "https://app.omie.com.br/api/v1/estoque/consulta/"
        payload = json.dumps({
        "call": "ListarMovimentoEstoque",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [
            {
                "nPagina": '{}'.format(pageNum),
                "nRegPorPagina":100,
                "codigo_local_estoque":0,
                "idProd":0,
                "lista_local_estoque":"",
                "dDtInicial": '{}'.format(hoje),
                "dDtFinal": "31/12/2050"        
        
            }
            ]
        })
        headers = { 
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        result = response.json()

        consulta_results = consulta_results + [result]

        resp = consulta_results
        print('Extraindo Página: ''{}'.format(pageNum))

        df = pd.json_normalize(resp, record_path=['movProdutoListar'])

        df = df.assign(Origem=ep)

    # print(df)

    # # Conecta no Postgre e alimenta a Stage movimentos
    engine=create_engine("postgresql+psycopg2://postgres:""{}".format(os.getenv('password')) + "@""{}".format(os.getenv('host')) + ":5432/postgres")

    df.to_sql(name='stg_movimento_estoque', con=engine, if_exists='append',index=False,schema='stage')

fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicioEstoque))


### Processo Atualiza dados de Matéria Prima


print('Bases Materia Prima Omie. Etapa 3 de 6')
inicio_mat = datetime.datetime.now()


url = "https://app.omie.com.br/api/v1/geral/produtos/"


###### Trunca a tabela stg_movimentos_omie

conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

conn.autocommit = True

cur = conn.cursor()
cur.execute(f"TRUNCATE TABLE STAGE.STG_MATERIA_PRIMA")
conn.commit()
cur.close()
conn.close()


empresas = ['Dustre','ES']
for empresa in empresas:
    ep = empresa
    if ep == 'Dustre':
        app_key = app_key_dustre
        app_secret = app_secret_dustre
        familia = familia_dustre
    else:
        app_key = app_key_ES
        app_secret = app_secret_ES
        familia = familia_ES

    payload = json.dumps({
    "call": "ListarProdutos",
    "app_key": app_key,
    "app_secret": app_secret,
    "param": [
        {
        "pagina": 1,
        "registros_por_pagina": 50,
        "apenas_importado_api":"N",
        "filtrar_apenas_omiepdv":"N",
        "filtrar_apenas_familia":familia,
        "inativo": "N"
        
        }
    ]
    })
    headers = { 
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    result =  response.json()

    # print(result)

    totalPaginas = result['total_de_paginas']
    totalRegistros = result['total_de_registros']
    print('>>>>>> Base Materia Prima: ''{}'.format(ep) +' <<<<<<<')
    print('Total de Paginas: ''{}'.format(totalPaginas))
    print('Total de Registros: ''{}'.format(totalRegistros))

    consulta_results = []

    for pageNum in range(1,int(totalPaginas)+1):
        url = "https://app.omie.com.br/api/v1/geral/produtos/"
        payload = json.dumps({
        "call": "ListarProdutos",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [
            {
                "pagina": '{}'.format(pageNum),
                "registros_por_pagina": 50,
                "apenas_importado_api":"N",
                "filtrar_apenas_omiepdv":"N",
                "filtrar_apenas_familia":familia,
                "inativo": "N"
        
            }
            ]
        })
        headers = { 
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        result = response.json()

        consulta_results = consulta_results + [result]

        resp = consulta_results
        print('Extraindo Página: ''{}'.format(pageNum) + ' de ''{}'.format(totalPaginas))

        df = pd.json_normalize(resp, record_path=['produto_servico_cadastro'])

        df = df.assign(Origem=ep)

    engine=create_engine("postgresql+psycopg2://postgres:""{}".format(os.getenv('password')) + "@""{}".format(os.getenv('host')) + ":5432/postgres")

    df.to_sql(name='stg_materia_prima', con=engine, if_exists='append',index=False,schema='stage')

fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_mat))



##### Processo Atualiza dados da Posição de Estoque


print('Bases Posicao Estoque Omie. Etapa 4 de 6')
inicio_posEst = datetime.datetime.now()

url = "https://app.omie.com.br/api/v1/estoque/consulta/"


  ###### Trunca a tabela stg_pos_estoque

conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

conn.autocommit = True

cur = conn.cursor()
cur.execute(f"TRUNCATE TABLE stage.stg_pos_estoque")
conn.commit()
cur.close()
conn.close()


###### Processo alimenta os dados de posição de estoque ES
data_atual = date.today()
hoje = '{}/{}/{}'.format(data_atual.day, data_atual.month,data_atual.year)
print('Extraindo dados da posicao de estoque de:' '{}'.format(hoje))


payload = json.dumps({
"call": "ListarPosEstoque",
"app_key": os.getenv('app_key_ES'),
"app_secret": os.getenv('app_secret_ES'),
"param": [
    {
    "nPagina": 1,
    "nRegPorPagina": 50,
    "dDataPosicao": '{}'.format(hoje),
    "cExibeTodos": "N",
    "codigo_local_estoque": 0
    }
]
})
headers = { 
'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

result =  response.json()

# print(result)

totalPaginas = result['nTotPaginas']
totalRegistros = result['nTotRegistros']
print('>>>>>> Processo Posicao de Estoque <<<<<<<')
print('Total de Paginas: ''{}'.format(totalPaginas))
print('Total de Registros: ''{}'.format(totalRegistros))

consulta_results = []

for pageNum in range(1,int(totalPaginas)+1):
    url = "https://app.omie.com.br/api/v1/estoque/consulta/"
    payload = json.dumps({
    "call": "ListarPosEstoque",
    "app_key": os.getenv('app_key_ES'),
    "app_secret": os.getenv('app_secret_ES'),
    "param": [
        {
            "nPagina": '{}'.format(pageNum),
            "nRegPorPagina": 50,
            "dDataPosicao": '{}'.format(hoje),
            "cExibeTodos": "N",
            "codigo_local_estoque": 0
        }
        ]
    })
    headers = { 
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)

    result = response.json()

    consulta_results = consulta_results + [result]

    resp = consulta_results
    print('Extraindo Página: ''{}'.format(pageNum) + ' de ''{}'.format(totalPaginas))

df = pd.json_normalize(resp, record_path=['produtos'])

df = df.assign(Origem='ES',dDataPosicao = date.today())

# print(df)

# # Conecta no Postgre e alimenta a Stage movimentos
engine=create_engine("postgresql+psycopg2://postgres:""{}".format(os.getenv('password')) + "@""{}".format(os.getenv('host')) + ":5432/postgres")

df.to_sql(name='stg_pos_estoque', con=engine, if_exists='append',index=False,schema='stage')

fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_posEst))



###Processo que alimenta dados de Categorias


print('Bases Categorias Omie. Etapa 5 de 6')
inicio_cat = datetime.datetime.now()

url = "https://app.omie.com.br/api/v1/geral/categorias/"

###### Trunca a tabela stg_categorias_omie

conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

conn.autocommit = True

cur = conn.cursor()
cur.execute(f"DROP TABLE STAGE.STG_CATEGORIAS_OMIE")

conn.commit()
cur.close()
conn.close()


###### Processo alimenta os dados de categorias

empresas = ['Dustre','Forge','ES']
for empresa in empresas:
    ep = empresa
    if ep == 'Dustre':
        app_key = app_key_dustre
        app_secret = app_secret_dustre
    elif ep == 'Forge':
        app_key = app_key_forge
        app_secret = app_secret_forge
    else:
        app_key = app_key_ES
        app_secret = app_secret_ES


    payload = json.dumps({
    "call": "ListarCategorias",
    "app_key": app_key,
    "app_secret": app_secret,
    "param": [
        {
        "pagina": 1,
        "registros_por_pagina": 50
        # "lDadosCad":"S",
        # "dDtAltDe": "01/11/2022",
        # "dDtAltAte":"31/12/2050"
        }
    ]
    })
    headers = { 
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    result =  response.json()

    totalPaginas = result['total_de_paginas']
    totalRegistros = result['total_de_registros']
    print('>>>>>> Processo categorias: ''{}'.format(ep) +' <<<<<<<')
    print('Total de Paginas: ''{}'.format(totalPaginas))
    print('Total de Registros: ''{}'.format(totalRegistros))

    consulta_results = []
    for pageNum in range(1,int(totalPaginas)+1):
        url = "https://app.omie.com.br/api/v1/geral/categorias/"
        payload = json.dumps({
        "call": "ListarCategorias",
        "app_key": app_key,
        "app_secret": app_secret,
        "param": [
            {
            "pagina": '{}'.format(pageNum),
            "registros_por_pagina": 50
            # "lDadosCad":"S",
            # "dDtAltDe": "01/11/2022",
            # "dDtAltAte":"31/12/2050"
            
            }
        ]
        })
        headers = { 
        'Content-Type': 'application/json'
        }
        response = requests.request("POST", url, headers=headers, data=payload)

        result = response.json()

        consulta_results = consulta_results + [result]

        resp = consulta_results
        print('Extraindo Página: ''{}'.format(pageNum) + ' de ''{}'.format(totalPaginas))
        df = pd.json_normalize(resp, record_path=['categoria_cadastro'],meta_prefix=False)
        df = df.assign(Origem=ep)
        df = df[(df['conta_inativa'] == 'N')]

        # Conecta no Postgre e alimenta a Stage categorias

    engine=create_engine("postgresql+psycopg2://postgres:""{}".format(os.getenv('password')) + "@""{}".format(os.getenv('host')) + ":5432/postgres")
    df.to_sql(name='stg_categorias_omie', con=engine, if_exists='append',index=False,schema='stage')

fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_cat))



#Execução de Procedures
inicio_SP = datetime.datetime.now()
print('Procedures. Etapa 6 de 6')
print("Iniciando Execução de Procedures...")

    
conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

conn.autocommit = True

cur = conn.cursor()
print('sp_aux_materia_prima')
cur.execute('CALL datamart.sp_aux_materia_prima()')
print('sp_aux_categorias')
cur.execute('CALL datamart.sp_aux_categorias()')
print('Historico Movimento Estoque')
cur.execute('CALL historico.sp_historico_movestoque()')
print('Historico Movimentos')
cur.execute('CALL historico.sp_histmovimentos()')
print('sp_facmovimentosdre')
cur.execute('CALL datamart.sp_facmovimentosdre()')
print('sp_fact_pos_estoque')
cur.execute('CALL datamart.sp_fact_pos_estoque()')
print('sp_fact_pag_rec')
cur.execute('CALL datamart.sp_fact_pag_rec()')
print('sp_total_pagar_receber')
cur.execute('CALL datamart.sp_total_pagar_receber()')
print('atualizaHist_TCF')
cur.execute('CALL datamart.atualizahist_tcf()')


conn.commit()
cur.close()
conn.close()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_SP))
print('Tempo total: ''{}'.format(fim-inicio))
print('Processo finalizado com sucesso')