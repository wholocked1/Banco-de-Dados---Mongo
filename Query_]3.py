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
        print(tab)
