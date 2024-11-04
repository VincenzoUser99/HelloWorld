from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27017/')
db = client.MyDatabase
collection=db['test_1']




def update_df(df, col, col1):
    for _, row in df.iterrows():
        query = {col: row[col]}
        update = {"$set": {col1: row[col1] }}
        collection.update_many(query, update)



def update_df_array(df,col,col1,col2):
    for _,row in df.iterrows():
        query = {col: row[col]}
        concatenated_value = f"{row[col2]}"
        update = {'$addToSet': {col1: concatenated_value}}
        collection.update_many(query,update)


def raccogli_aggiornamenti(df, query_campi, update_campi,campo1,campo):
    """
    Funzione che raccoglie tutte le query e gli aggiornamenti in un array.

    Parametri:
    df: DataFrame contenente i dati.
    query_campi: Dizionario che mappa i campi MongoDB ai nomi delle colonne del DataFrame per la query.
    update_campi: Dizionario che mappa i campi MongoDB ai nomi delle colonne del DataFrame per l'update.

    Ritorna:
    Una lista di dizionari contenente le query e gli aggiornamenti.
    """
    aggiornamenti = []  # Array per memorizzare tutte le operazioni di update

    # Itera sulle righe del DataFrame
    for _, row in df.iterrows():
        # Costruzione della query dinamica
        query = {campo1: row[colonna] for campo, colonna in query_campi.items()}

        # Costruzione dell'oggetto update dinamico
        update = {
            '$addToSet': {campo: row[colonna] for campo, colonna in update_campi.items()}
        }

        # Aggiungi query e update alla lista
        aggiornamenti.append({'query': query, 'update': update})

    return aggiornamenti


def esegui_aggiornamenti(db, aggiornamenti):
    """
    Funzione che esegue gli aggiornamenti su MongoDB basandosi su una lista di query e update.

    Parametri:
    db: Il database MongoDB che contiene la collezione 'comuni_storico'.
    aggiornamenti: Lista di dizionari contenenti le query e gli aggiornamenti da eseguire.
    """
    # Itera su tutti gli aggiornamenti raccolti
    for operazione in aggiornamenti:
        query = operazione['query']
        update = operazione['update']

        # Esegui l'aggiornamento nel database
        collection.update_many(query, update)

    print("Tutti gli aggiornamenti sono stati eseguiti.")


def formatta_numero(numero):
    try:
        
        numero_int = int(numero)
        return f"{numero_int:06d}"
    except(ValueError, TypeError):
        return

# Applica la funzione a tutta la colonna


# Aggiungiamo una colonna 'stato' in base alla presenza nei due elenchi
def flag_soppressione(df_effettivi, df_soppressi):
    # Convertiamo l'elenco dei comuni soppressi in un set per ottimizzare la ricerca
    soppressi_set = set(df_soppressi['comune_soppresso'].tolist())

    # Funzione che controlla se un comune è soppresso
    def verifica_stato(comune):
        if comune in soppressi_set:
            return 'cessato'
        else:
            return 'attivo'

    # Applichiamo la funzione per determinare il flag "attivo" o "cessato"
    df_effettivi['stato'] = df_effettivi['comune_effettivo'].apply(verifica_stato)

    return df_effettivi






