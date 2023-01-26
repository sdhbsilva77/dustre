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


def materia_prima():
  url = "https://app.omie.com.br/api/v1/geral/produtos/"

  app_key_dustre = os.getenv('app_key_dustre')
  app_secret_dustre = os.getenv('app_secret_dustre')
  familia_dustre = os.getenv('familia_dustre')

  app_key_ES = os.getenv('app_key_ES')
  app_secret_ES = os.getenv('app_secret_ES')
  familia_ES = os.getenv('familia_ES')



  ###### Trunca a tabela stg_movimentos_omie

  conn = psycopg2.connect(database=os.getenv('database'),user=os.getenv('user'),password=os.getenv('password'),host=os.getenv('host'),port=os.getenv('port'))

  conn.autocommit = True

  cur = conn.cursor()
  cur.execute(f"TRUNCATE TABLE STAGE.STG_MATERIA_PRIMA")
  conn.commit()
  cur.close()
  conn.close()


  ###### Processo alimenta os dados de materia prima Dustre

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