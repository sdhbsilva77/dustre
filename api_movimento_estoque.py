import requests
import json
import pandas as pd
import numpy as np
import sys
from collections import defaultdict
from sqlalchemy import create_engine
import psycopg2
from datetime import date, timedelta
from dotenv import load_dotenv
import os


load_dotenv()

def movimento_estoque():
    url = "https://app.omie.com.br/api/v1/estoque/consulta/"

    app_key_dustre = os.getenv('app_key_dustre')
    app_secret_dustre = os.getenv('app_secret_dustre')

    app_key_forge = os.getenv('app_key_forge')
    app_secret_forge = os.getenv('app_secret_forge')

    app_key_ES = os.getenv('app_key_ES')
    app_secret_ES = os.getenv('app_secret_ES')


    ###### Trunca a tabela stg_movimentos_omie

    conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

    conn.autocommit = True

    cur = conn.cursor()
    cur.execute(f"TRUNCATE TABLE STAGE.STG_MOVIMENTO_ESTOQUE")
    conn.commit()
    cur.close()
    conn.close()


    ###### Processo alimenta os dados de materia prima Dustre

    data_atual = date.today() + timedelta(days=-30)
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