import pandas as pd
from sqlalchemy import create_engine
import psycopg2

def exec_procedures():

    print("Iniciando Execução de Procedures...")

    
    conn = psycopg2.connect(database="postgres",user="postgres",password="Imt:d{Ck=R*Z9&0_",host="35.247.246.160",port='5432')

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
   
   
    conn.commit()
    cur.close()
    conn.close()