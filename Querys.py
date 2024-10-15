from pymongo import MongoClient
import pandas as pd

def get_database(): #conexão com o banco de dados do Mongo DB
    conex =  "mongodb+srv://anacarolnami:jK1YAqsC8yNd638g@ana.cdnv7ly.mongodb.net/Ana"

    client = MongoClient(conex)

    return  client['Faculdade']

db = get_database()
op = 0
while True:
    print("Coloque o número da query que deseja realizar: (clique 0 para sair)")
    op = int(input())
    if(op == 0): #Sai do while
        break
    if(op == 1): #query 1 - histórico escolar de qualquer aluno, retornando o código e nome da disciplina, semestre e ano que a disciplina foi cursada e nota final
        aluno = db["Hist_Aluno"] # conecta a tabela de hitórico do aluno
        item = aluno.find()
        tab = pd.DataFrame(item)
        #converte para string
        #tab["id_aluno"] = tab["id_aluno"].astype("string")
        #tab["codigo"] = tab["codigo"].astype("string")
        disc = db["Disciplina"] # conecta na tabela de Disciplina
        item2 = disc.find()
        other = pd.DataFrame(item2)
        other["codigo"] = other["codigo"].astype("string")
        hist = tab.join(other.set_index('codigo'), on='codigo', how='left', lsuffix='_hist', rsuffix='_disciplina') #dá o join nas duas tabelas com as chaves de _id e código
        print(hist[hist.id_aluno == '8490'].filter(items=['id_aluno', 'semestre_hist', 'ano', 'nota', 'codigo_hist', 'nome'])) #mostra somente as partes necessárias
    if(op == 2): # Query 2 - histórico de disciplinas ministradas por qualquer professor, com semestre e ano
        prof = db['Hist_Professor'] # conecta na tabela de Histórico do Professor
        item = prof.find()
        tab = pd.DataFrame(item)
        #Transforma as informações em string
        tab["id_prof"] = tab["id_prof"].astype("string")
        tab["codigo"] = tab["codigo"].astype("string")
        disc = db["Disciplina"] # conecta na tabela de Disciplinas
        item2 = disc.find()
        other = pd.DataFrame(item2)
        other["_id"] = other["_id"].astype("string")
        hist = tab.join(other.set_index('_id'), on='codigo', how='left', lsuffix='_hist', rsuffix='_disciplina') # dá join nas tabelas nos ids _id e código
        print(hist[hist.id_prof == '66ecae55360fa8661b083fe9'].filter(items=['id_prof', 'semestre_hist', 'ano', 'nome', 'codigo_disciplina'])) #mostra somenta as partes necessárias
    if(op == 3): # Query 3 - listar alunos que já se formaram (foram aprovados em todos os cursos de uma matriz curricular) em um determinado semestre de um ano
        aluno = db['Aluno'] # conecta a tabela de aluno
        item = aluno.find()
        tab = pd.DataFrame(item)
        print(tab[tab.formado == "true"].filter(items=['_id', 'nome', 'ciclo'])) # mostra a tabela das pessoas que já se formaram
    if(op == 4): # Query 4 - listar todos os professores que são chefes de departamento, junto com o nome do departamento
        dep = db['Departamento'] # conecta a tabela de Departamento
        item = dep.find()
        tab = pd.DataFrame(item)
        #tranforma a informação em string
        tab["id_prof"] = tab['id_prof'].astype("string")
        prof = db['Professor'] # conecta a tabela de professor
        item2 = prof.find()
        other = pd.DataFrame(item2)
        #tranforma a informação em string
        other["_id"] = other['_id'].astype("string")
        d = tab.join(other.set_index('_id'), on = 'id_prof', how = 'left', lsuffix="_dep", rsuffix="_prof") #da join nas tables pelos ids _id e id_prof
        print(d)
    if(op == 5): # Query 5 - saber quais alunos formaram um grupo de TCC e qual professor foi o orientador
        aluno = db['Aluno'] # conecta a tabela aluno
        item = aluno.find()
        tab = pd.DataFrame(item)
        #tranforma a informação em string
        tab["id_tcc"] = tab['id_tcc'].astype("string")
        tcc = db['TCC'] # conecta a tabela do TCC
        item2 = tcc.find()
        other = pd.DataFrame(item2)
        #tranforma a informação em string
        other["_id"] = other['_id'].astype("string")
        d = tab.join(other.set_index('_id'), on = 'id_tcc', how = 'left', lsuffix="_aluno", rsuffix="_tcc") # dá join por meio dos ids
        prof = db['Professor']
        item3 = prof.find()
        other2 = pd.DataFrame(item3)
        g = d.join(other2.set_index('_id'), on= 'id_prof', how= "left", rsuffix="_prof") # dá join a partir dos ids
        g['nome_prof'] = g['nome_prof'].astype("string")
        print(g.filter(items=["nome", "titulo", "nome_prof"]))

        
