from pymongo import MongoClient
import pandas as pd

def get_database():
    conex =  "mongodb+srv://anacarolnami:jK1YAqsC8yNd638g@ana.cdnv7ly.mongodb.net/Ana"

    client = MongoClient(conex)

    return  client['Faculdade']

db = get_database()
op = 0
while True:
    print("Coloque o número da query que deseja realizar:")
    op = int(input())
    if(op == 0):
        break
    if(op == 1):
        aluno = db["Hist_Aluno"]
        item = aluno.find()
        tab = pd.DataFrame(item)
        tab["id_aluno"] = tab["id_aluno"].astype("string")
        tab["codigo"] = tab["codigo"].astype("string")
        disc = db["Disciplina"]
        item2 = disc.find()
        other = pd.DataFrame(item2)
        other["_id"] = other["_id"].astype("string")
        hist = tab.join(other.set_index('_id'), on='codigo', how='left', lsuffix='_hist', rsuffix='_disciplina')
        print(hist[hist.id_aluno == '66ecab73360fa8661b05ddc8'].filter(items=['id_aluno', 'semestre_hist', 'ano', 'nota', 'nome', 'codigo_disciplina']))
    if(op == 2):
        prof = db['Hist_Professor']
        item = prof.find()
        tab = pd.DataFrame(item)
        tab["id_prof"] = tab["id_prof"].astype("string")
        tab["codigo"] = tab["codigo"].astype("string")
        disc = db["Disciplina"]
        item2 = disc.find()
        other = pd.DataFrame(item2)
        other["_id"] = other["_id"].astype("string")
        hist = tab.join(other.set_index('_id'), on='codigo', how='left', lsuffix='_hist', rsuffix='_disciplina')
        print(hist[hist.id_prof == '66ecae55360fa8661b083fe9'].filter(items=['id_prof', 'semestre_hist', 'ano', 'nome', 'codigo_disciplina']))
    if(op == 3):
        aluno = db['Aluno']
        item = aluno.find()
        tab = pd.DataFrame(item)
        print(tab[tab.formado == "true"].filter(items=['_id', 'nome', 'ciclo']))
    if(op == 4): #parcialmente feito, não está linkando com o professor direito
        dep = db['Departamento']
        item = dep.find()
        tab = pd.DataFrame(item)
        tab["id_prof"] = tab['id_prof'].astype("string")
        prof = db['Professor']
        item2 = prof.find()
        other = pd.DataFrame(item2)
        d = tab.join(other.set_index('_id'), on = 'id_prof', how = 'left', lsuffix="_dep", rsuffix="_prof")
        print(d)