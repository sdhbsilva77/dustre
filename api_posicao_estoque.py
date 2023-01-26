import requests
import json
import pandas as pd
import numpy as np
import sys
from collections import defaultdict
from sqlalchemy import create_engine
import psycopg2
from datetime import date
from dotenv import load_dotenv
import os


load_dotenv()

def posicao_estoque():
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

    # df.to_excel('C:\Dustre\movimentos.xlsx',index=False)