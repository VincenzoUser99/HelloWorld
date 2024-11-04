import requests
from io import BytesIO
from io import StringIO
import pandas as pd
from pymongo import MongoClient
import json
import numpy as np
import pymongo
from pymongo import InsertOne
from definizione_update import update_df
from definizione_update import formatta_numero
from pymongo import UpdateOne





client=MongoClient ('mongodb://localhost:27017/')
db=client.MyDatabase
collection = db['test_1']
df_test_1 = pd.DataFrame(db.test_1.find({}))

data={"Content-Type": "application/json,charset=utf-8"}

variazione_territoriale_req=requests.post('https://situas.istat.it/ShibO2Module/api/Report/Spool/1861-03-17/105?&PageNum=1&PageSize=4384&dataA=2024-10-03',json=data)
cambi_denominazione_req = requests.post('https://situas.istat.it/ShibO2Module/api/Report/Spool/1861-03-17/104?&PageNum=1&PageSize=2762&dataA=2024-09-25', json=data)
comuni_soppressi_req = requests.post("https://situas.istat.it/ShibO2Module/api/Report/Spool/1861-03-17/128?&PageNum=1&PageSize=2634", json=data )
comuni_variazioni_terr_req = requests.post('https://situas.istat.it/ShibO2Module/api/Report/Spool/1861-03-17/103?&PageNum=1&PageSize=4562&dataA=2024-10-07',json=data)
elenco_comuni_req=requests.get('https://www.istat.it/storage/codici-unita-amministrative/Elenco-comuni-italiani.xls')

df_variazioni_territoriali = pd.DataFrame(json.loads(variazione_territoriale_req.text)['body'])
df_cambi_denominazione = pd.DataFrame(json.loads(cambi_denominazione_req.text)['body'])
df_comuni_soppressi = pd.DataFrame(json.loads(comuni_soppressi_req.text)['body'])
df_comuni_variazioni_terr = pd.DataFrame(json.loads(comuni_variazioni_terr_req.text)['body'])
df_elenco_comuni = pd.read_excel(BytesIO(elenco_comuni_req.content))

df_variazioni_territoriali=df_variazioni_territoriali.rename(columns={'COMUNE':'comune','PRO_COM_T':'codice_comune','COMUNE_REL':'comune_effettivo','PRO_COM_T_REL':'codice_comune_formato_numerico','DATA_INIZIO_AMMINISTRATIVA':'data_evento','SIGLA_UTS':'sigla_provincia','COD_VARIAZIONE':'tipo_variazione','SIGLA_UTS_REL':'sigla_provincia_effettiva'})
df_elenco_comuni=df_elenco_comuni.rename(columns={'Denominazione in italiano':'comune_effettivo','Codice Comune formato alfanumerico':'codice_comune_formato_alfanumerico','Codice Comune formato numerico':'codice_comune_formato_numerico','Sigla automobilistica':'sigla_provincia_effettiva'})
df_cambi_denominazione=df_cambi_denominazione.rename(columns={'COMUNE_REL':'comune_effettivo','PRO_COM_T_REL':'codice_comune_formato_numerico','COMUNE':'comune','PRO_COM_T':'codice_comune_precedente','DATA_INIZIO_AMMINISTRATIVA':'data_evento','SIGLA_UTS':'sigla_provincia','SIGLA_UTS_REL':'sigla_provincia_effettiva'})
df_comuni_soppressi=df_comuni_soppressi.rename(columns={'COMUNE':'comune','PRO_COM_T':'codice_comune_soppresso','COMUNE_REL':'comune_effettivo','PRO_COM_T_REL':'codice_comune_formato_numerico','DATA_INIZIO_AMMINISTRATIVA':'data_evento','FLAG_ES_SCORPORO':'flag_es_scorporo','SIGLA_UTS':'sigla_provincia','COD_VARIAZIONE':'tipo_variazione','SIGLA_UTS_REL':'sigla_provincia_effettiva'})
df_comuni_variazioni_terr=df_comuni_variazioni_terr.rename(columns={'SIGLA_UTS':'sigla_provincia','COMUNE':'comune','DATA_INIZIO_AMMINISTRATIVA':'data_evento','COD_VARIAZIONE_M':'tipo_variazione','PRO_COM_T':'codice_comune','COD_VARIAZIONE':'tipo_variazione'})



df_elenco_comuni['sigla_provincia_effettiva'] = df_elenco_comuni['sigla_provincia_effettiva'].fillna('NA')
df_variazioni_territoriali['comune']=df_variazioni_territoriali['comune'].str.upper().str.replace("'", "")
df_variazioni_territoriali['comune_effettivo']=df_variazioni_territoriali['comune_effettivo'].str.upper().str.replace("'", "")
df_cambi_denominazione['comune_effettivo'] = df_cambi_denominazione['comune_effettivo'].str.upper().str.replace("'", "")
df_elenco_comuni['comune_effettivo'] = df_elenco_comuni['comune_effettivo'].str.upper().str.replace("'", "")
df_cambi_denominazione['comune']=df_cambi_denominazione['comune'].str.upper().str.replace("'", "")
df_comuni_soppressi['comune_effettivo']=df_comuni_soppressi['comune_effettivo'].str.upper().str.replace("'", "")
df_comuni_soppressi['comune']=df_comuni_soppressi['comune'].str.upper().str.replace("'", "")
df_comuni_variazioni_terr['comune']=df_comuni_variazioni_terr['comune'].str.upper().str.replace("'","")
df_elenco_comuni['codice_comune_formato_numerico']=df_elenco_comuni['codice_comune_formato_numerico'].apply(formatta_numero)
df_comuni_soppressi['codice_comune_formato_numerico']=df_comuni_soppressi['codice_comune_formato_numerico'].apply(formatta_numero)
df_cambi_denominazione['codice_comune_formato_numerico']=df_cambi_denominazione['codice_comune_formato_numerico'].apply(formatta_numero)
df_comuni_soppressi['codice_comune_soppresso']=df_comuni_soppressi['codice_comune_soppresso'].apply(formatta_numero)
df_cambi_denominazione['codice_comune_precedente']=df_cambi_denominazione['codice_comune_precedente'].apply(formatta_numero)
df_comuni_variazioni_terr['codice_comune']=df_comuni_variazioni_terr['codice_comune'].apply(formatta_numero)
df_comuni_soppressi['flag_es_scorporo']=df_comuni_soppressi['flag_es_scorporo'].replace(np.nan, None)
df_variazioni_territoriali['codice_comune_formato_numerico']=df_variazioni_territoriali['codice_comune_formato_numerico'].apply(formatta_numero)
df_variazioni_territoriali['codice_comune']=df_variazioni_territoriali['codice_comune'].apply(formatta_numero)


#inizializziamo un count
counts = 0
#lista di comune_effettivo
denominazioni = df_elenco_comuni['comune_effettivo'].replace("'","").dropna().tolist()
#inseriamo in una lista tutte le nuove_denominazioni
esistenti = collection.find({"comune_effettivo": {"$in": denominazioni}}, {"comune_effettivo": 1})
#Un set in Python è una collezione non ordinata e senza duplicati di elementi si chiama hush table per l'archiviazione di ricerca
esistenti_set = {doc['comune_effettivo'] for doc in esistenti}
# Vengono inseriti solo i dati che devo essere inseriti nel Database
nuovi_da_inserire = [den for den in denominazioni if den not in esistenti_set]
# l'if ha la funzione di inserire quei dati nel db con il bulk_write è un metodo per eseguire più operazioni
if nuovi_da_inserire:
    operazioni = [InsertOne({"comune_effettivo": den}) for den in nuovi_da_inserire]
    collection.bulk_write(operazioni)
    counts += len(nuovi_da_inserire)
    print(f"Inserted {len(nuovi_da_inserire)} new documents for comune_effettivo.")

print(f"Total new documents inserted: {counts}")

col='comune_effettivo'
col1='codice_comune_formato_numerico'
df=df_elenco_comuni

update_df(
    df,col,col1
)
col='comune_effettivo'
col1='sigla_provincia_effettiva'
df=df_elenco_comuni

update_df(
    df,col,col1
)

counts = 0

# Estrai i valori necessari dal DataFrame, rimuovendo apostrofi e valori nulli
denominazioni = (
    df_cambi_denominazione[['comune_effettivo', 'codice_comune_formato_numerico', 'sigla_provincia_effettiva']]
    .replace("'", "")
    .dropna()
    .drop_duplicates()
    .to_dict('records')
)
# Recupera i documenti esistenti dal database
esistenti = collection.find(
    {"comune_effettivo": {"$in": [den['comune_effettivo'] for den in denominazioni]}},  # Condizione di ricerca per 'comune'
    { "comune_effettivo": 1}  # Estrarre solo il campo 'comune'
)

# Creazione del set con i valori esistenti di 'comune'
esistenti_set = {doc['comune_effettivo'] for doc in esistenti}

# Vengono inseriti solo i dati che devono essere inseriti nel database
nuovi_da_inserire = [
    den for den in denominazioni
    if den['comune_effettivo'] not in esistenti_set
]

# Se ci sono nuove denominazioni da inserire, esegui l'inserimento nel database
if nuovi_da_inserire:
    operazioni = [
        InsertOne({
            "comune_effettivo": den['comune_effettivo'],
            "codice_comune_formato_numerico": den['codice_comune_formato_numerico'],
            "sigla_provincia_effettiva": den['sigla_provincia_effettiva']

        })
        for den in nuovi_da_inserire
    ]
    collection.bulk_write(operazioni)
    counts += len(nuovi_da_inserire)
    print(f"Inserted {len(nuovi_da_inserire)} new documents for comune_combi_den.")

print(f"Total new documents inserted: {counts}")


counts = 0

# Estrai i valori necessari dal DataFrame, rimuovendo apostrofi e valori nulli
denominazioni = (
    df_comuni_soppressi[['comune_effettivo', 'codice_comune_formato_numerico', 'sigla_provincia_effettiva']]
    .replace("'", "")
    .dropna()
    .drop_duplicates()
    .to_dict('records')
)
# Recupera i documenti esistenti dal database
esistenti = collection.find(
    {"comune_effettivo": {"$in": [den['comune_effettivo'] for den in denominazioni]}},  # Condizione di ricerca per 'comune'
    { "comune_effettivo": 1}  # Estrarre solo il campo 'comune'
)

# Creazione del set con i valori esistenti di 'comune'
esistenti_set = {doc['comune_effettivo'] for doc in esistenti}

# Vengono inseriti solo i dati che devono essere inseriti nel database
nuovi_da_inserire = [
    den for den in denominazioni
    if den['comune_effettivo'] not in esistenti_set
]

# Se ci sono nuove denominazioni da inserire, esegui l'inserimento nel database
if nuovi_da_inserire:
    operazioni = [
        InsertOne({
            "comune_effettivo": den['comune_effettivo'],
            "codice_comune_formato_numerico": den['codice_comune_formato_numerico'],
            "sigla_provincia_effettiva": den['sigla_provincia_effettiva']

        })
        for den in nuovi_da_inserire
    ]
    collection.bulk_write(operazioni)
    counts += len(nuovi_da_inserire)
    print(f"Inserted {len(nuovi_da_inserire)} new documents for comune_soppresso.")

print(f"Total new documents inserted: {counts}")



# Inizializzazione del contatore per i documenti inseriti
counts = 0

# Estrai i valori necessari dal DataFrame, rimuovendo apostrofi e valori nulli
denominazioni = (
    df_cambi_denominazione[['comune', 'codice_comune_precedente', 'sigla_provincia']]
    .replace("'", "")
    .dropna()
    .drop_duplicates()
    .to_dict('records')
)

# Recupera i documenti esistenti dal database
esistenti = collection.find(
    {"comune": {"$in": [den['comune'] for den in denominazioni]}},  # Condizione di ricerca per 'comune'
    { "comune": 1}  # Estrarre solo il campo 'comune'
)

# Creazione del set con i valori esistenti di 'comune'
esistenti_set = {doc['comune'] for doc in esistenti}

# Vengono inseriti solo i dati che devono essere inseriti nel database
nuovi_da_inserire = [
    den for den in denominazioni
    if den['comune'] not in esistenti_set
]

if nuovi_da_inserire:
    operazioni = [
        InsertOne({
            "comune": den['comune'],
            "codice_precedente": den['codice_comune_precedente'],
            "sigla": den['sigla_provincia']
        })
        for den in nuovi_da_inserire
    ]
    collection.bulk_write(operazioni)
    counts += len(nuovi_da_inserire)
    print(f"Inserted {len(nuovi_da_inserire)} new documents for comune_combi_den.")

print(f"Total new documents inserted: {counts}")



counts = 0

# Estrai i valori necessari dal DataFrame, rimuovendo apostrofi e valori nulli
denominazioni = (
    df_comuni_soppressi[['comune', 'codice_comune_soppresso', 'sigla_provincia']]
    .replace("'", "")
    .dropna()
    .drop_duplicates()
    .to_dict('records')
)

# Recupera i documenti esistenti dal database
esistenti = collection.find(
    {"comune": {"$in": [den['comune'] for den in denominazioni]}},  # Condizione di ricerca per 'comune'
    { "comune": 1}  # Estrarre solo il campo 'comune'
)

# Creazione del set con i valori esistenti di 'comune'
esistenti_set = {doc['comune'] for doc in esistenti}

# Vengono inseriti solo i dati che devono essere inseriti nel database
nuovi_da_inserire = [
    den for den in denominazioni
    if den['comune'] not in esistenti_set
]

# Se ci sono nuove denominazioni da inserire, esegui l'inserimento nel database
if nuovi_da_inserire:
    operazioni = [
        InsertOne({
            "comune": den['comune'],
            "codice_precedente": den['codice_comune_soppresso'],
            "sigla": den['sigla_provincia']

        })
        for den in nuovi_da_inserire
    ]
    collection.bulk_write(operazioni)
    counts += len(nuovi_da_inserire)
    print(f"Inserted {len(nuovi_da_inserire)} new documents for comune_soppresso.")

print(f"Total new documents inserted: {counts}")


for _,row in df_cambi_denominazione.iterrows():
  try:
    query={'comune':row['comune'],'codice_comune_denominazione':row['codice_comune_precedente'],'sigla_provincia':row['sigla_provincia']}

    update={'$addToSet':{'nuova_denominazione':{'nome':row['comune_effettivo'],
            'codice':row['codice_comune_formato_numerico'],
            'data_evento':row['data_evento'],
            'sigla':row['sigla_provincia_effettiva'],
            }}}

    collection.update_many(query,update)
  except Exception as e:
      print(f"Errore durante l'elaborazione della riga: {row},{e}")
      continue


for _, row in df_comuni_soppressi.iterrows():
    try:
        # Definisci la query per trovare i documenti
        query = {
            'comune': row['comune_effettivo'],
            'codice_comune_soppresso': row['codice_comune_soppresso'],
            'sigla_provincia': row['sigla_provincia_effettiva']
        }
        # Definisci l'update utilizzando $addToSet
        update = {'$addToSet': {'comune_associato':{'nome': row['comune_effettivo'],
            'codice': row['codice_comune_formato_numerico'],
            'data_evento': row['data_evento'],
            'sigla': row['sigla_provincia_effettiva']
            }}}

        # Esegui l'update nel database
        collection.update_many(query, update)
    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: {row}, {e}")
        continue


operations = []

for _, row in df_cambi_denominazione.iterrows():
    try:
        query = {
            'comune_effettivo': row['comune_effettivo'],
            'codice_comune_formato_numerico': row['codice_comune_formato_numerico'],
            'sigla_provincia_effettiva': row['sigla_provincia_effettiva']
        }

        existing_doc = collection.find_one(query)

        # Controlla se l'elemento esiste già nel campo 'cambio_denominazione'
        if existing_doc:
            cambio_denominazione = existing_doc.get('cambio_denominazione', [])
            if row['comune'] not in [x['nome'] for x in cambio_denominazione]:
                update = {
                    '$addToSet': {
                        'cambio_denominazione': {
                            'nome': row['comune'],
                            'codice_precedente': row['codice_comune_precedente'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia']
                        }
                    }
                }
                operations.append(UpdateOne(query, update))
        else:

            update = {
                '$addToSet': {
                    'cambio_denominazione': {
                        'nome': row['comune'],
                        'codice_precedente': row['codice_comune_precedente'],
                        'data_evento': row['data_evento'],
                        'sigla': row['sigla_provincia']
                    }
                }
            }
            operations.append(UpdateOne(query, update))

    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: {row}, {e}")
        continue


if operations:
    try:
        result = collection.bulk_write(operations)
        print(f"Operazioni completate: {result.bulk_api_result}")
    except Exception as e:
        print(f"Errore durante l'esecuzione delle operazioni bulk: {e}")


for _, row in df_comuni_soppressi.iterrows():
    try:

        query = {
            'comune_effettivo': row['comune_effettivo'],
            'codice_comune_formato_numerico': row['codice_comune_formato_numerico'],
            'sigla_provincia_effettiva': row['sigla_provincia_effettiva']
        }

        existing_doc = collection.find_one(query)


        if existing_doc:
            comune_soppresso = existing_doc.get('comune_soppresso', [])
            if row['comune'] not in [x['nome'] for x in comune_soppresso]:
                update = {
                    '$addToSet': {
                        'comune_soppresso': {
                            'nome': row['comune'],
                            'codice_precedente': row['codice_comune_soppresso'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia'],
                            'flag_es_scorporo': row['flag_es_scorporo'],
                            'tipo_variazione': row['tipo_variazione']
                        }
                    }
                }
                operations.append(UpdateOne(query, update))

    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: {row}, {e}")
        continue


if operations:
    try:
        result = collection.bulk_write(operations)
        print(f"Operazioni completate: {result.bulk_api_result}")
    except Exception as e:
        print(f"Errore durante l'esecuzione delle operazioni bulk: {e}")

operations = []

for _, row in df_comuni_soppressi.iterrows():
    try:
        query = {
            'comune': row['comune'],
            'codice_precedente': row['codice_comune_soppresso'],
            'sigla': row['sigla_provincia']
        }

        existing_doc = collection.find_one(query)

        if existing_doc:

            comune_associato = existing_doc.get('nuova_denominazione', [])



            if row['comune_effettivo'] not in [x['nome'] for x in comune_associato]:
                update = {
                    '$addToSet': {
                        'comune_associato': {
                            'nome': row['comune_effettivo'],
                            'codice': row['codice_comune_formato_numerico'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia_effettiva'],
                            'flag_es_scorporo': row['flag_es_scorporo'],
                            'tipo_variazione': row['tipo_variazione']
                        }
                    }
                }
                operations.append(UpdateOne(query, update))


    except :
        continue

if operations:
    try:
        result = collection.bulk_write(operations)
        print(f"Operazioni completate: {result.bulk_api_result}")
    except Exception as e:
        print(f"Errore durante l'esecuzione delle operazioni bulk: {e}")

operations = []

for _, row in df_cambi_denominazione.iterrows():
    try:
        query = {
            'comune': row['comune'],
            'codice_precedente': row['codice_comune_precedente'],
            'sigla': row['sigla_provincia']
        }

        existing_doc = collection.find_one(query)



        if existing_doc:

            nuova_denominazione = existing_doc.get('nuova_denominazione', [])


            # Check if the new entry already exists
            if row['comune_effettivo'] not in [x['nome'] for x in nuova_denominazione]:
                update = {
                    '$addToSet': {
                        'nuova_denominazione': {
                            'nome': row['comune_effettivo'],
                            'codice': row['codice_comune_formato_numerico'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia_effettiva']
                        }
                    }
                }
                operations.append(UpdateOne(query, update))

        else:

            update = {
                '$addToSet': {
                    'nuova_denominazione': {
                        'nome': row['comune_effettivo'],
                        'codice': row['codice_comune_formato_numerico'],
                        'data_evento': row['data_evento'],
                        'sigla': row['sigla_provincia_effettiva']
                    }
                }
            }
            operations.append(UpdateOne(query, update))

    except :
        continue

if operations:
    try:
        result = collection.bulk_write(operations)
        print(f"Operazioni completate: {result.bulk_api_result}")
    except Exception as e:
        print(f"Errore durante l'esecuzione delle operazioni bulk: {e}")


#######
operations = []

for _, row in df_cambi_denominazione.iterrows():
    try:
        query = {
            'comune': row['comune_effettivo'],
            'codice_precedente': row['codice_comune_formato_numerico'],
            'sigla': row['sigla_provincia_effettiva']
        }

        existing_doc = collection.find_one(query)


        if existing_doc:
            cambio_denominazione = existing_doc.get('cambio_denominazione', [])
            if row['comune'] not in [x['nome'] for x in cambio_denominazione]:
                update = {
                    '$addToSet': {
                        'cambio_denominazione': {
                            'nome': row['comune'],
                            'codice_precedente': row['codice_comune_precedente'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia']
                        }
                    }
                }
                operations.append(UpdateOne(query, update))
        else:
            update = {
                '$addToSet': {
                    'cambio_denominazione': {
                        'nome': row['comune'],
                        'codice_precedente': row['codice_comune_precedente'],
                        'data_evento': row['data_evento'],
                        'sigla': row['sigla_provincia']
                    }
                }
            }
            operations.append(UpdateOne(query, update))

    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: {row}, Query: {query}, {e}")
        continue

if operations:
    try:
        result = collection.bulk_write(operations)
        print(f"Operazioni completate: {result.bulk_api_result}")
    except Exception as e:
        print(f"Errore durante l'esecuzione delle operazioni bulk: {e}")

operations = []

for _, row in df_comuni_soppressi.iterrows():
    try:
        query = {
            'comune': row['comune_effettivo'],
            'codice_precedente': row['codice_comune_formato_numerico'],
            'sigla': row['sigla_provincia_effettiva']
        }

        existing_doc = collection.find_one(query)

        if existing_doc:
            comune_soppresso = existing_doc.get('comune_soppresso', [])
            if row['comune'] not in [x['nome'] for x in comune_soppresso]:
                update = {
                    '$addToSet': {
                        'comune_soppresso': {
                            'nome': row['comune'],
                            'codice_precedente': row['codice_comune_soppresso'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia'],
                            'flag_es_scorporo': row['flag_es_scorporo'],
                            'tipo_variazione': row['tipo_variazione']
                        }
                    }
                }
                operations.append(UpdateOne(query, update))

    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: {row.to_dict()}, {e}")
        continue

if operations:
    try:
        result = collection.bulk_write(operations)
        print(f"Operazioni completate: {result.bulk_api_result}")
    except Exception as e:
        print(f"Errore durante l'esecuzione delle operazioni bulk: {e}")
operations = []

for _, row in df_cambi_denominazione.iterrows():
    try:
        # Construct the query to find existing documents with a matching 'nuova_denominazione'
        query = {
            'nuova_denominazione': {
                '$elemMatch': {
                    'nome': row['comune_effettivo'],
                    'codice': row['codice_comune_formato_numerico'],
                    'sigla': row['sigla_provincia_effettiva']
                }
            }
        }

        existing_doc = collection.find_one(query)

        if existing_doc:
            nuova_denominazione = existing_doc.get('nuova_denominazione', [])

            # Check if 'row['comune']' is not already in 'nuova_denominazione'
            if row['comune_effettivo'] not in [x['nome'] for x in nuova_denominazione]:
                update = {
                    '$addToSet': {
                        'nuova_denominazione': {'nome': row['comune_effettivo'],
                                                'codice': row['codice_comune_formato_numerico'],
                                                'data_evento': row['data_evento'],
                                                'sigla': row['sigla_provincia_effettiva']}
                    }
                }

                # Update the document with the new entry
                result = collection.update_many({'nuova_denominazione':query}, update)


    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _, row in df_cambi_denominazione.iterrows():
    try:
        # Construct the query to find existing documents with a matching 'nuova_denominazione'
        query = {
            'nuova_denominazione': {
                '$elemMatch': {
                    'nome': row['comune_effettivo'],
                    'codice': row['codice_comune_formato_numerico'],
                    'sigla': row['sigla_provincia_effettiva']
                }
            }
        }

        existing_doc = collection.find_one(query)

        if existing_doc:
            nuova_denominazione = existing_doc.get('nuova_denominazione', [])

            # Check if 'row['comune']' is not already in 'nuova_denominazione'
            if row['comune_effettivo'] not in [x['nome'] for x in nuova_denominazione]:
                update = {
                    '$addToSet': {
                        'nuova_denominazione': {
                            'nome': row['comune'],
                            'codice': row['codice_comune_precedente'],
                            'data_evento': row['data_evento'],
                            'sigla': row['sigla_provincia']
                        }
                    }
                }

                # Update the document with the new entry
                result = collection.update_many({'nuova_denominazione':query}, update)


    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: , Errore: {e}")



for _,row in df_variazioni_territoriali.iterrows():

  try:
    query={'comune_effettivo': row['comune_effettivo'],'codice_comune_formato_numerico':row['codice_comune_formato_numerico'],'sigla_provincia_effettiva':row['sigla_provincia_effettiva']}
    update={'$addToSet':{'variazione_territoriale':{'sigla_var':row['sigla_provincia'],'codice_var':row['codice_comune'],'comune':row['comune'],'data_evento':row['data_evento']}}}

    collection.update_many(query,update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_variazioni_territoriali.iterrows():

  try:
    query={'$elemMatch': {'nome': row['comune_effettivo'],'codice_precedente':row['codice_comune_formato_numerico'],'sigla_provincia':row['sigla_provincia_effettiva']}}
    update={'$addToSet':{'variazione_territoriale':{'sigla_var':row['sigla_provincia'],'codice_var':row['codice_comune'],'comune':row['comune'],'data_evento':row['data_evento']}}}

    collection.update_many({'comune_soppresso':query},update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_variazioni_territoriali.iterrows():

  try:
    query={'$elemMatch': {'nome': row['comune_effettivo'],'codice_precedente':row['codice_comune_formato_numerico'],'sigla_provincia':row['sigla_provincia_effettiva']}}
    update={'$addToSet':{'variazione_territoriale':{'sigla_var':row['sigla_provincia'],'codice_var':row['codice_comune'],'comune':row['comune'],'data_evento':row['data_evento']}}}

    collection.update_many({'cambio_denominazione':query},update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: , Errore: {e}")



for _,row in df_variazioni_territoriali.iterrows():

  try:
    query={'$elemMatch': {'comune': row['comune_effettivo'],'codice_var':row['codice_comune_formato_numerico'],'sigla_var':row['sigla_provincia_effettiva']}}
    update={'$addToSet':{'variazione_territoriale':{'sigla_var':row['sigla_provincia'],'codice_var':row['codice_comune'],'comune':row['comune'],'data_evento':row['data_evento']}}}

    collection.update_many({'variazione_territoriale':query},update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_variazioni_territoriali.iterrows():

  try:
    query={'$elemMatch': {'comune': row['comune_effettivo'],'codice_var':row['codice_comune_formato_numerico'],'sigla_var':row['sigla_provincia_effettiva']}}
    update={'$addToSet':{'variazione_territoriale':{'sigla_var':row['sigla_provincia'],'codice_var':row['codice_comune'],'comune':row['comune'],'data_evento':row['data_evento']}}}

    collection.update_many({'variazione_territoriale':query},update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: , Errore: {e}")


for _,row in df_comuni_variazioni_terr.iterrows():
    try:
      query={'$elemMatch': {'nome':row['comune'],'codice_precedente':row['codice_comune'],'sigla':row['sigla_provincia']}}
      update = {
          '$set': {
              'comune_soppresso.$.modalità_scorporo': row['tipo_variazione']
          }
      }

      collection.update_many({'comune_soppresso':query},update)
    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_comuni_variazioni_terr.iterrows():
    try:
      query={'$elemMatch': {'nome':row['comune'],'codice_precedente':row['codice_comune'],'sigla':row['sigla_provincia']}}
      update = {
          '$set': {
              'cambio_denominazione.$.modalità_scorporo': row['tipo_variazione']
          }
      }

      collection.update_many({'cambio_denominazione':query},update)
    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_comuni_variazioni_terr.iterrows():
    try:
      query={'comune':row['comune'],'codice_precedente':row['codice_comune'],'sigla':row['sigla_provincia']}
      update={'$set':{'modalità_scorporo':row['tipo_variazione']}}

      collection.update_many(query,update)
    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_comuni_variazioni_terr.iterrows():
    try:
      query={'comune_effettivo':row['comune'],'codice_comune_formato_numerico':row['codice_comune'],'sigla_provincia_effettiva':row['sigla_provincia']}
      update={'$set':{'modalità_scorporo':row['tipo_variazione']}}

      collection.update_many(query,update)
    except Exception as e:
        print(f"Errore durante l'elaborazione della riga: , Errore: {e}")

for _,row in df_cambi_denominazione.iterrows():

  try:
    query = {'comune': row['comune_effettivo'], 'codice_precedente': row['codice_comune_formato_numerico'],'sigla': row['sigla_provincia_effettiva']}
    update={'$addToSet':{'nuova_denominazione':{'codice_comune_formato_numerico':row['codice_comune_formato_numerico'],'sigla_provincia_effettiva':row['sigla_provincia_effettiva'],'comune':row['comune_effettivo']}}}

    collection.update_many(query,update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: {row}, Errore: {e}")

for _,row in df_comuni_soppressi.iterrows():

  try:
    query={'comune': row['comune_effettivo'],'codice_precedente':row['codice_comune_formato_numerico'],'sigla':row['sigla_provincia_effettiva']}
    update={'$addToSet':{'comune_associato':{'comune':row['comune_effettivo'],'codice_comune_formato_numerico':row['codice_comune_formato_numerico'],'sigla_provincia_effettiva':row['sigla_provincia_effettiva']}}}

    collection.update_many(query,update)
  except Exception as e:
    print(f"Errore durante l'elaborazione della riga: {row}, Errore: {e}")
df_test_1=db.test_1.find({})

comuni_effettivi = df_elenco_comuni['comune_effettivo'].tolist()

documents = db.test_1.find()


for document in documents:
    document_id = document['_id']
    update_query = {'$set': {}}

    if 'comune_effettivo' in document:

        if document['comune_effettivo'] in comuni_effettivi:
            update_query['$set']['attivo'] = True
        else:
            update_query['$set']['attivo'] = False

    if 'comune' in document:
        update_query['$set']['attivo'] = False


    if 'cambio_denominazione' in document:

        updated_cambio_denominazione = [
            {**item, 'attivo': False} for item in document['cambio_denominazione']
        ]
        update_query['$set']['cambio_denominazione'] = updated_cambio_denominazione


    if 'comune_soppresso' in document:

        update_query['$set']['comune_soppresso'] = [
            {**item, 'attivo': False} for item in document['comune_soppresso']
        ]

    # Esegui l'aggiornamento solo se ci sono modifiche da applicare
    if update_query['$set']:
        db.test_1.update_many({'_id': document_id}, update_query)

client.close()

