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
        # colocamos a tabela departamento como string, e deu certo :O
        dep = db['Departamento']
        item = dep.find()
        tab = pd.DataFrame(item)
        tab["id_prof"] = tab['id_prof'].astype("string")
        prof = db['Professor']
        item2 = prof.find()
        other = pd.DataFrame(item2)
        other["_id"] = other['_id'].astype("string")
        d = tab.join(other.set_index('_id'), on = 'id_prof', how = 'left', lsuffix="_dep", rsuffix="_prof")
        print(d)
    if(op == 5):
        # não terminado query fizemos o primeiro join, não fizemos o segundo e esta dando erro no key id_prof :(
        aluno = db['Aluno']
        item = aluno.find()
        tab = pd.DataFrame(item)
        tab["id_tcc"] = tab['id_tcc'].astype("string")
        tcc = db['TCC']
        item2 = tcc.find()
        other = pd.DataFrame(item2)
        other["_id"] = other['_id'].astype("string")
        d = tab.join(other.set_index('_id'), on = 'id_tcc', how = 'left', lsuffix="_aluno", rsuffix="_tcc")
        prof = db['Professor']
        item3 = prof.find()
        other2 = pd.DataFrame(item3)
        #other2["_id"] = other2['_id'].astype("string")
        #other2["id_prof"] = other2['id_prof'].astype("string")
        g = d.join(other2.set_index('_id'), on= 'id_prof', how= "left", rsuffix="_prof")
        #g["id_prof_prof"] = g['id_prof'].astype("string")
        g['nome_prof'] = g['nome_prof'].astype("string")
        print(g.filter(items=["nome", "titulo", "nome_prof"]))
        #print(g.info())
        #print(other2)
        
