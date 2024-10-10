# Banco-de-Dados---Mongo

**Nomes:**<br>
- Ana Carolina Cogo Nami - 22.124.084-9<br>
- Enzo Lima Cassin - 22.124.083-1
- Jéssica Martins de Jesus - 22.124.096-3
- João Vitor Flôres  - 11.120.313-9
<br><br>

# **Atenção na hora de realizar as Querys!!**
**Até o arquivo estando em Python é necessário o `pip install pymongo` na máquina que for usar, sendo ela virtual ou física. Se essa instalação não for feita, ela não conseguirá conectar no banco de dados!**

# Explicação
Estamos usando a plataforma MongoDB para construir o banco de dados de forma gratuita e para colocar as informações e testar o código que estamos implementando estamos usando a ferramenta DBeaver.<br><br>
O código no arquivo 'tabelas.json' está as tabelas criadas para cada uma das partes do diagrama.<br><br>
O código no arquivo 'ALTER.json' está as alter tables que são usadas para poder pegar chave estrangeira das outras tabelas. <br><br>
O código no arquivo 'ALUNO.json' está as informações criadas aleatoriamente usando a ferramenta [Mockaroo]([url](https://www.mockaroo.com)) para criar as informações para serem carregadas no banco de dados.<br><br>
O código no arquivo 'Professor.json' está as informações criadas aleatoriamente usando a ferramenta Mockaroo para criar informações aleatórias para serem carregadas no banco de dados.<br><br>
O código no arquivo 'DISCIPLINAS.json' está as informações construídas a mão para gerar as disciplinas de cada curso para serem carregadas no banco de dados. No sistema terá os cursos de durações diferentes (1 semestre, 2 semestres, 3 semestres e 4 semestres).<br><br>
O código no arquivo 'Curso.json' está as informações criadas manualmente dos cursos que estarão disponíveis nessa faculdade ficticia para serem carregadas no banco de dados. <br><br>
O código no arquivo 'DEPARTAMENTO.json' está as informações que foram construídas a mão para gerar os departamentos que dará cada aula para serem carregados no banco de dados. <br><br>
O código no arquivo 'Hist_Professor.json' está as informações referentes ao histórico de aulas dada pelos professores de um departamento gerados aleatoriamento usando a ferramenta Mockaroo para criar as informações de forma controlada e aleatória para serem carregadas no banco de dados. <br><br>
O código no arquivo 'MATRIZ.json' está as informações que linkam as informações presentes no curso com as informações das disciplinas para serem carregados no banco de dados. Esse código foi criado a mão para melhor controle.<br><br>
O código no arquivo 'TCC.json' está as informações que dos TCCs, como título, id e id do professor para carregar no banco de dados. Ela foi criada de forma controlada pela plataforma Mockaroo.<br><br>
O código no arquivo 'HIST_ALUNO.json' está as informações de histórico do aluno com as notas, semestre, ano, id do aluno e código da disciplina. Esse arquivo foi criado a mão.<br><br>
O arquivo "Querys.py" está as querys com um tipo de interface pelo terminal que você pode escolher qual query que deseja realizar na ordem que está documentado ao decorrer do código.<br><br>


**Diagrama relacional:**
``` mermaid
erDiagram
    ALUNO {
        string nome
        int id_aluno
        int ciclo
        boolean formado
        int id_tcc
        int id_curso
    }
    PROFESSOR {
        string nome
        int id_departamento
        int id_professor
    }
    DEPARTAMENTO {
        string nome
        int id_departamento
        int id_prof_chefe
    }
    HISTORICO_ALUNO{
        int id_aluno
        int id_disciplina
        int semestre
        int ano
        float nota
    }
    DISCIPLINA{
        string nome
        string codigo
        int id_departamento
        int semestre
    }
    HISTORICO_PROF{
        int id_prof
        int id_disciplina
        int semestre
        int ano
    }
    CURSO{
        string nome
        int duracao
        int id_departamento
        int id_curso
    }
    TCC{
        int id_tcc
        string titulo
        int id_prof
    } 
    MATRIZ{
        int id_disc
        int id_curso
        int semestre
    }
    ALUNO }o--|| CURSO : tem
    ALUNO ||--|| HISTORICO_ALUNO: tem
    PROFESSOR ||--|| HISTORICO_PROF: tem
    DEPARTAMENTO ||--o{ PROFESSOR: tem
    HISTORICO_ALUNO }|--|{ MATRIZ: tem
    HISTORICO_PROF }|--|{ MATRIZ: tem
    TCC }|--|| PROFESSOR: tem
    ALUNO }|--o| TCC: tem
    MATRIZ ||--|{ DISCIPLINA: tem
    MATRIZ ||--|| CURSO: tem
    DEPARTAMENTO ||--o{ DISCIPLINA: tem

  
```

**Referência:** <br>
Mockaroo: https://www.mockaroo.com <br>
dbeaver: https://dbeaver.io <br>
cockroach labs: https://www.cockroachlabs.com <br>
https://products.aspose.app/cells/conversion/sql-to-json
