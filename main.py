import datetime
from api_materia_prima import materia_prima
from api_omie import movimentos
from api_posicao_estoque import posicao_estoque
from procedures import exec_procedures
from categorias import categorias
from api_movimento_estoque import movimento_estoque

# >>>>>>>>>> ATUALIZA AS BASES OMIE NO POSTGRE <<<<<<<<<<<<<<<

print('Iniciando Atualização em 6 etapas')

inicio = datetime.datetime.now()
print('Inicio: ''{}'.format(inicio))

print('Bases Movimentos Omie. Etapa 1 de 6')
movimentos()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio))

print('Bases Movimento Estoque Omie. Etapa 2 de 6')
movimento_estoque()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio))

inicio_mat = datetime.datetime.now()
print('Bases Materia Prima Omie. Etapa 3 de 6')
materia_prima()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_mat))

inicio_posEst = datetime.datetime.now()
print('Bases Posicao Estoque Omie. Etapa 4 de 6')
posicao_estoque()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_posEst))

inicio_cat = datetime.datetime.now()
print('Bases Categorias Omie. Etapa 5 de 6')
categorias()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_cat))

#Execução de Procedures
inicio_SP = datetime.datetime.now()
print('Procedures. Etapa 6 de 6')
exec_procedures()
fim = datetime.datetime.now()
print('Tempo de Execução: ''{}'.format(fim-inicio_SP))
print('Tempo total: ''{}'.format(fim-inicio))
print('Processo finalizado com sucesso')