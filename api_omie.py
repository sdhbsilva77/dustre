import requests
import json
import pandas as pd
import numpy as np
import sys
from collections import defaultdict
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv
import os


load_dotenv()

def movimentos():
  url = "https://app.omie.com.br/api/v1/financas/mf/"

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
  cur.execute(f"TRUNCATE TABLE STAGE.STG_MOVIMENTOS_OMIE_DUSTRE")
  cur.execute(f"TRUNCATE TABLE STAGE.STG_MOVIMENTOS_OMIE_FORGE")
  cur.execute(f"TRUNCATE TABLE STAGE.STG_MOVIMENTOS_OMIE_ES")
  conn.commit()
  cur.close()
  conn.close()


  ###### Processo alimenta os dados de movimentos Dustre
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