#!/usr/bin/env python
# coding: utf-8

# <span style="color: white; font-weight: bold; font-size: 50px;">Récuperation de la liste des DGEFP</span>

# In[3]:


import requests

dataset_id = "liste-publique-des-of-v2"
export_url = f"https://dgefp.opendatasoft.com/api/v2/catalog/datasets/{dataset_id}/exports/csv"
params = {
    "delimiter": ";",  
    "list_separator": ",", 
    "quote_all": "false", 
    "with_bom": "true" 
}

response = requests.get(export_url, params=params)

if response.status_code == 200:
    csv_path = 'all_records_export.csv'
    with open(csv_path, 'wb') as file:
        file.write(response.content)
    print(f"Les enregistrements ont été exportés et sauvegardés dans '{csv_path}'.")
else:
    print(f"Erreur lors de l'exportation : {response.status_code}")


# In[4]:


import pandas as pd

csv_path = 'all_records_export.csv'

try:
    df = pd.read_csv(csv_path, delimiter=';')
    print(df.head())
    print(f"Nombre total d'enregistrements exportés : {df.shape[0]}")
    print(f"Nombre total de colonnes : {df.shape[1]}")
except FileNotFoundError:
    print(f"Le fichier '{csv_path}' n'existe pas.")
except Exception as e:
    print(f"Une erreur s'est produite : {e}")


# In[5]:


df.to_csv("listesdesqualiopietnon.csv", index=False)
df


# In[6]:


df


# In[ ]:





# In[ ]:





# <span style="color: white; font-weight: bold; font-size: 50px;">Récuperation de la liste des entreprises - Yav</span>

# In[9]:


import time
import configparser
import requests
import pandas as pd

def CrmBitrix(content='', params=[], result='', pause_duration=1, max_retries=3):
    config = configparser.ConfigParser()
    config.read('Config.ini')

    if 'BITRIX' not in config:
        raise ValueError('Section BITRIX manquante dans le fichier \'config.ini\'')

    BITRIX_DOMAIN = config.get('BITRIX', 'BITRIX_DOMAIN')
    BITRIX_TOKEN = config.get('BITRIX', 'BITRIX_TOKEN')

    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    params_str = '&'.join(params)
    BitrixUrl = f'https://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/{content}?{params_str}&start='
    
    listIds, listTITLE, listEmails, listNumeros, listDATE_CREATE = [], [], [], [], []
    listActivite1, listActivite2, listActivite3 = [], [], []
    listAdresse, listCodePostale, listVille, listId, listSiret, listidd = [], [], [], [],[], []

    start = 0
    while True:
        url = BitrixUrl + str(start)
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                break  
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = pause_duration * (2 ** attempt)
                    print(f'Rate limit exceeded. Retrying in {wait_time} seconds...')
                    time.sleep(wait_time)
                else:
                    raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')
            except requests.exceptions.RequestException as e:
                raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')

        

        data = response.json()
        if 'result' in data:
            contacts = data['result']
            for contact in contacts:
                if result == 'email':
                    if 'EMAIL' in contact:
                        email = contact['EMAIL'][0]['VALUE']
                        listEmails.append(email)
                        if 'PHONE' in contact:
                            mesNumeros = contact['PHONE'][0]['VALUE']
                            listNumeros.append(mesNumeros)
                        else:
                            listNumeros.append('')
                        
                elif result == 'title': 
                    listTITLE.append(contact.get('TITLE', ''))
                    listEmails.append(contact.get('EMAIL', [{}])[0].get('VALUE', ''))
                    listNumeros.append(contact.get('PHONE', [{}])[0].get('VALUE', ''))
                    listId.append(contact.get('ID', ''))
                    listActivite1.append(contact.get('UF_CRM_1686295056', ''))
                    listActivite2.append(contact.get('UF_CRM_1686295079', ''))
                    listActivite3.append(contact.get('UF_CRM_1686295100', ''))
                    listAdresse.append(contact.get('UF_CRM_1686295131', ''))
                    listCodePostale.append(contact.get('UF_CRM_1686295169', ''))
                    listVille.append(contact.get('UF_CRM_1686295194', ''))
                    listDATE_CREATE.append(contact.get('DATE_CREATE', ''))
                    listSiret.append(contact.get('UF_CRM_1686295022', ''))
                    listidd.append(contact.get('UF_CRM_1722346888', ''))
                else:
                    return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

        if 'next' in data:
            start = data['next']
        else:
            break

    if result == 'email':
        df_btx = pd.DataFrame({'email': listEmails, 'phone': listNumeros})
    elif result == 'title':
        df_btx = pd.DataFrame({
            'ID': listId,
            'CompanyId': listId,
            'nom': listTITLE,
            "Email": listEmails,
            "Crée": listDATE_CREATE,
            "Mobile": listNumeros,
            "Activite1": listActivite1,
            "Activite2": listActivite2,
            "Activite3": listActivite3,
            "Adresse": listAdresse,
            "CodePostale": listCodePostale,
            "Ville": listVille,
            "Siret": listSiret,
            "IDS": listidd
        })
    else:
        return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

    return df_btx


dfb = CrmBitrix(
    content='crm.company.list.json',
    params=[
        'select[]=ID',
        'select[]=ID',
        'select[]=TITLE',
        'select[]=EMAIL',
        'select[]=DATE_CREATE',
        'select[]=UF_CRM_1686295056',
        'select[]=UF_CRM_1686295079',
        'select[]=UF_CRM_1686295100',
        'select[]=UF_CRM_1686295131',
        'select[]=UF_CRM_1686295169',
        'select[]=UF_CRM_1686295194', 
        'select[]=UF_CRM_1686295022',
        'select[]=UF_CRM_1722346888',
        
    ],
    result="title"
)
dfb


# In[8]:


dfb.to_csv("company.csv", index=False)
dfb.drop_duplicates()
dfb


# In[9]:


import pandas as pd
import requests
import configparser
from requests_oauthlib import OAuth1
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
defaultCrmProspectListUrl = "http://yav.bitrix24.fr/rest/1/m13iijnhd3blq35m/crm.company.userfield.list?ID="

listUrls = []
listeId = []
listID = []
listValuesource = []
listIDd = []

datas = {}

responses = requests.get(defaultCrmProspectListUrl)
        
def testedv():
    if responses.status_code == 200:
        datas = responses.json()
        companyfield = datas['result'][10]['LIST']

        for source in companyfield:
            listID.append(source['ID'])
            listValuesource.append(source['VALUE'])
           
            

            df_btx = pd.DataFrame(list(zip(listID, listValuesource)),
                              columns = ["IDS",'Statut'])

        return df_btx


# In[10]:


dfdd = testedv()
dfdd


# In[11]:


df_Bitrixyav = dfb.merge(dfdd, how='left')
df_Bitrixyav.drop_duplicates()
df_Bitrixyav.to_csv('companyyavlist.csv', index=False, encoding='utf-8')
df_Bitrixyav


# In[36]:


import pandas as pd

file1_path = 'companyyavlist.csv'
file2_path = 'listesdesqualiopietnon.csv'

try:
    companyyav_df = pd.read_csv(file1_path)
    all_records_export_df = pd.read_csv(file2_path, delimiter=',', on_bad_lines='skip', low_memory=False)
except pd.errors.ParserError as e:
    print(f"Erreur lors de la lecture des fichiers CSV : {e}")

if 'companyyav_df' in locals() and 'all_records_export_df' in locals():
    print("Colonnes disponibles dans 'listesdesqualiopietnon.csv' :")
    print(all_records_export_df.columns)

    if 'siretetablissementdeclarant' in all_records_export_df.columns:
        companyyav_df['Siret'] = companyyav_df['Siret'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        all_records_export_df['siretetablissementdeclarant'] = all_records_export_df['siretetablissementdeclarant'].astype(str).str.strip()

        print(f"Exemples de SIRET dans 'companyyav.csv' : {companyyav_df['Siret'].head()}")
        print(f"Exemples de SIRET dans 'listesdesqualiopietnon.csv' : {all_records_export_df['siretetablissementdeclarant'].head()}")

        unique_siret_companyyav = companyyav_df['Siret'].nunique()
        unique_siret_all_records = all_records_export_df['siretetablissementdeclarant'].nunique()

        print(f"Nombre de SIRET uniques dans 'companyyav.csv': {unique_siret_companyyav}")
        print(f"Nombre de SIRET uniques dans 'listesdesqualiopietnon.csv': {unique_siret_all_records}")

        common_values = companyyav_df[companyyav_df['Siret'].isin(all_records_export_df['siretetablissementdeclarant'])]
        print(f"Nombre de correspondances trouvées: {common_values.shape[0]}")

        merged_df = pd.merge(common_values, all_records_export_df, left_on='Siret', right_on='siretetablissementdeclarant')

        print("\nInformations des valeurs communes dans 'Siret' et 'siretetablissementdeclarant':")
        print(merged_df.head()) 

        missing_siret_in_all_records = companyyav_df[~companyyav_df['Siret'].isin(all_records_export_df['siretetablissementdeclarant'])]
        print(f"Nombre de SIRET dans 'companyyav.csv' non trouvés dans 'listesdesqualiopietnon.csv': {missing_siret_in_all_records.shape[0]}")
        print(missing_siret_in_all_records.head())
    else:
        print("La colonne 'siretetablissementdeclarant' n'existe pas dans le fichier 'listesdesqualiopietnon.csv'.")
else:
    print("Les fichiers n'ont pas pu être chargés correctement.")


# In[37]:


merged_df.to_csv("Liste_des_dgefp_qui_existe_dans_bitrix_et_dans_la_liste_des_dgefp.csv", index=False)
merged_df


# In[ ]:





# In[ ]:





# <span style="color: white; font-weight: bold; font-size: 50px;">Je vérifie si les entreprises YAV conservent leur statut Qualiopi dans DFFEP (indiquant un changement de statut).</span>

# In[14]:


import pandas as pd

df_dgefp = pd.read_csv('Liste_des_dgefp_qui_existe_dans_bitrix_et_dans_la_liste_des_dgefp.csv')
df_qualiopi = pd.read_csv('listesdesqualiopietnon.csv')

print("Échantillon des valeurs dans la colonne 'Siret' du premier CSV :")
print(df_dgefp['Siret'].head())

print("\nÉchantillon des valeurs dans la colonne 'siretetablissementdeclarant' du deuxième CSV :")
print(df_qualiopi['siretetablissementdeclarant'].head())

print("\nType de la colonne 'Siret' : ", df_dgefp['Siret'].dtype)
print("Type de la colonne 'siretetablissementdeclarant' : ", df_qualiopi['siretetablissementdeclarant'].dtype)

df_dgefp['Siret'] = df_dgefp['Siret'].astype(str).str.strip()
df_qualiopi['siretetablissementdeclarant'] = df_qualiopi['siretetablissementdeclarant'].astype(str).str.strip()

print("\nAprès conversion si nécessaire :")
print(df_dgefp['Siret'].head())
print(df_qualiopi['siretetablissementdeclarant'].head())


# <span style="color: white; font-weight: bold; font-size: 50px;">Récuperation des non qualiopi DGEFP.</span>

# In[15]:


import pandas as pd

df = pd.read_csv("listesdesqualiopietnon.csv", encoding='utf-8', low_memory=False)

colonnes_a_verifier = [
    'certifications_actionsdeformation', 
    'certifications_bilansdecompetences', 
    'certifications_vae', 
    'certifications_actionsdeformationparapprentissage'
]

df_nan = df[colonnes_a_verifier].isna()

df_nan_info = df[df_nan.any(axis=1)]

df_nan_info.to_csv("Liste_non_qualiopi_DGEFP.csv", index=False)
df_nan_info


# <span style="color: white; font-weight: bold; font-size: 50px;">Récuperation des Non qualiopi YAV.</span>

# In[16]:


import pandas as pd

df_dgefp = pd.read_csv('companyyavlist.csv')

df_dgefp['Siret'] = df_dgefp['Siret'].apply(lambda x: '{:.0f}'.format(x))

df_pas_qualiopi = df_dgefp[df_dgefp['Statut'] == 'Pas Qualiopi']

if not df_pas_qualiopi.empty:
    print("Entreprises avec le statut 'Pas Qualiopi':")
    print(df_pas_qualiopi)
else:
    print("Aucune entreprise avec le statut 'Pas Qualiopi' n'a été trouvée.")


# In[17]:


df_pas_qualiopi.to_csv("Liste_Pas_qualiopi_YAV.csv", index=False)
df_pas_qualiopi


# In[18]:


import pandas as pd

df_pas_qualiopi_yav = pd.read_csv('Liste_Pas_qualiopi_YAV.csv')
df_non_qualiopi = pd.read_csv('Liste_non_qualiopi_DGEFP.csv')
df_pas_qualiopi_yav['Siret'] = df_pas_qualiopi_yav['Siret'].apply(lambda x: '{:.0f}'.format(x))
df_non_qualiopi['siretetablissementdeclarant'] = df_non_qualiopi['siretetablissementdeclarant'].apply(lambda x: '{:.0f}'.format(x))
df_non_matching = df_pas_qualiopi_yav[~df_pas_qualiopi_yav['Siret'].isin(df_non_qualiopi['siretetablissementdeclarant'])]

if not df_non_matching.empty:
    print("Les entreprises dont les 'Siret' de Liste_Pas_qualiopi_YAV ne se trouvent PAS dans Liste_non_qualiopi_DGEFP :")
    print(df_non_matching)
else:
    print("Toutes les entreprises de Liste_Pas_qualiopi_YAV ont un 'Siret' correspondant dans Liste_non_qualiopi_DGEFP.")


# In[19]:


df_non_matching.to_csv('Non_Matching_Siret_YAV_Results.csv', index=False)
df_non_matching


# In[20]:


import pandas as pd

file_path = 'Non_Matching_Siret_YAV_Results.csv'

df = pd.read_csv(file_path)

colonnes_a_conserver = [
    'ID', 'nom', 'Email', 'Crée', 'Mobile', 'Activite1', 'Activite2', 'Activite3', 'Adresse', 'CodePostale', 'Ville', 'Siret','Statut'
]

df_filtré = df[colonnes_a_conserver]

df_filtré


# In[21]:


df_filtré.to_csv('df_filtre_Non_Matching_Siret_YAV_Results.csv', index=False)
df_filtré


# In[ ]:





# <span style="color: white; font-weight: bold; font-size: 50px;">Traitement des entreprises Pas Qualiopi (Yav) qui ont changé de statut voir si elles sont Qualiopi ou fermé.</span>

# In[28]:


import pandas as pd

file1_path = 'df_filtre_Non_Matching_Siret_YAV_Results.csv'
file2_path = 'listesdesqualiopietnon.csv'

try:
    companyyav_df = pd.read_csv(file1_path)
    all_records_export_df = pd.read_csv(file2_path, delimiter=',', on_bad_lines='skip', low_memory=False)
except pd.errors.ParserError as e:
    print(f"Erreur lors de la lecture des fichiers CSV : {e}")

if 'companyyav_df' in locals() and 'all_records_export_df' in locals():
    print("Colonnes disponibles dans 'listesdesqualiopietnon.csv' :")
    print(all_records_export_df.columns)

    if 'siretetablissementdeclarant' in all_records_export_df.columns:
        companyyav_df['Siret'] = companyyav_df['Siret'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        all_records_export_df['siretetablissementdeclarant'] = all_records_export_df['siretetablissementdeclarant'].astype(str).str.strip()

        print(f"Exemples de SIRET dans 'companyyav.csv' : {companyyav_df['Siret'].head()}")
        print(f"Exemples de SIRET dans 'listesdesqualiopietnon.csv' : {all_records_export_df['siretetablissementdeclarant'].head()}")

        unique_siret_companyyav = companyyav_df['Siret'].nunique()
        unique_siret_all_records = all_records_export_df['siretetablissementdeclarant'].nunique()

        print(f"Nombre de SIRET uniques dans 'companyyav.csv': {unique_siret_companyyav}")
        print(f"Nombre de SIRET uniques dans 'listesdesqualiopietnon.csv': {unique_siret_all_records}")

        common_values = companyyav_df[companyyav_df['Siret'].isin(all_records_export_df['siretetablissementdeclarant'])]
        print(f"Nombre de correspondances trouvées: {common_values.shape[0]}")

        merged_dfs = pd.merge(common_values, all_records_export_df, left_on='Siret', right_on='siretetablissementdeclarant')

        print("\nInformations des valeurs communes dans 'Siret' et 'siretetablissementdeclarant':")
        print(merged_dfs.head()) 

        missing_siret_in_all_recordss = companyyav_df[~companyyav_df['Siret'].isin(all_records_export_df['siretetablissementdeclarant'])]
        print(f"Nombre de SIRET dans 'companyyav.csv' non trouvés dans 'listesdesqualiopietnon.csv': {missing_siret_in_all_records.shape[0]}")
        print(missing_siret_in_all_recordss.head())
    else:
        print("La colonne 'siretetablissementdeclarant' n'existe pas dans le fichier 'listesdesqualiopietnon.csv'.")
else:
    print("Les fichiers n'ont pas pu être chargés correctement.")


# In[38]:


merged_dfs.to_csv("Liste_des_entreprises_qui_ont_change_de_statut_et_qui_se_trouve_dans_dgefp.csv", index=False)
merged_dfs


# In[52]:


import pandas as pd

df = pd.read_csv("Liste_des_entreprises_qui_ont_change_de_statut_et_qui_se_trouve_dans_dgefp.csv", encoding='utf-8', low_memory=False)

colonnes_a_verifier = [
    'certifications_actionsdeformation', 
    'certifications_bilansdecompetences', 
    'certifications_vae', 
    'certifications_actionsdeformationparapprentissage'
]

df_non_vide = df.dropna(subset=colonnes_a_verifier, how='all')

df_non_vide.to_csv("Les_qualiopi_dans_la_liste_des_entreprises_qui_ont_change_leur_statut.csv", index=False)
df_non_vide


# In[53]:


import pandas as pd

file_path = 'Les_qualiopi_dans_la_liste_des_entreprises_qui_ont_change_leur_statut.csv'
df = pd.read_csv(file_path)

colonnes_a_conserver = [
    'ID', 'nom', 'Email', 'Crée', 'Mobile', 'Activite1', 'Activite2', 'Activite3', 
    'Adresse', 'CodePostale', 'Ville', 'Siret', 'Statut'
]

df_filtrer = df[colonnes_a_conserver]

df_filtrer['Statut'] = df_filtrer['Statut'].replace('Pas Qualiopi', 'Qualiopi')

df_filtrer.to_csv("Liste_nn_modified.csv", index=False)
df_filtrer


# In[54]:


import time
import configparser
import requests
import pandas as pd

def CrmBitrix(content='', params=[], result='', pause_duration=1, max_retries=3):
    print("Début de la récupération des données de Bitrix24...")
    
    config = configparser.ConfigParser()
    config.read('config.ini')

    if 'BITRIX' not in config:
        raise ValueError('Section BITRIX manquante dans le fichier \'config.ini\'')

    BITRIX_DOMAIN = config.get('BITRIX', 'BITRIX_DOMAIN')
    BITRIX_TOKEN = config.get('BITRIX', 'BITRIX_TOKEN')

    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    print(f"Utilisation du domaine Bitrix: {BITRIX_DOMAIN}")

    params_str = '&'.join(params)
    BitrixUrl = f'https://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/{content}?{params_str}&start='
    
    listNoms, listIds = [], []

    start = 0
    while True:
        url = BitrixUrl + str(start)
        print(f"Envoi de la requête à l'URL: {url}")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = pause_duration * (2 ** attempt)
                    print(f'Limite de taux dépassée. Nouvelle tentative dans {wait_time} secondes...')
                    time.sleep(wait_time)
                else:
                    raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')
            except requests.exceptions.RequestException as e:
                raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')

        data = response.json()
        if 'result' in data:
            contacts = data['result']
            print(f"Nombre de contacts récupérés: {len(contacts)}")
            for contact in contacts:
                if result == 'title': 
                    listNoms.append(contact.get('TITLE', ''))
                    listIds.append(contact.get('ID', ''))
                else:
                    return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

        if 'next' in data:
            start = data['next']
        else:
            break

    if result == 'title':
        df_btx = pd.DataFrame({'ID': listIds, 'nom': listNoms})
    else:
        return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

    print("Récupération des données de Bitrix24 terminée.")
    return df_btx

def get_statut_value(statut):
    statut_mapping = {
        'Pas qualiopi': '140',
        'Qaliopi': '142',
        'Qaliopi à jour': '144',
        'Qaliopi perdu':'146'
    }
    return statut_mapping.get(statut, '142') 

def update_bitrix_status(df_local, df_bitrix):
    print("Début de la mise à jour des statuts dans Bitrix24...")

    config = configparser.ConfigParser()
    config.read('config.ini')

    BITRIX_DOMAIN = config.get('BITRIX', 'BITRIX_DOMAIN')
    BITRIX_TOKEN = config.get('BITRIX', 'BITRIX_TOKEN')

    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    for index, row in df_local.iterrows():
        nom = row['nom']
        statut = row['Statut']
        
        print(f"Vérification du nom: {nom} avec statut: {statut}")
        
        match = df_bitrix[df_bitrix['nom'] == nom]
        if not match.empty:
            bitrix_id = match.iloc[0]['ID']
            statut_value = get_statut_value(statut)
            update_url = f'https://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/crm.company.update.json'
            data = {
                'ID': bitrix_id,
                'fields': {
                    'UF_CRM_1722346888': statut_value
                }
            }
            print(f"Envoi de la mise à jour pour l'ID Bitrix: {bitrix_id} avec statut: {statut_value}")
            response = requests.post(update_url, json=data)
            if response.status_code == 200:
                print(f'Statut mis à jour avec succès pour {nom} dans Bitrix24')
            else:
                print(f'Échec de la mise à jour du statut pour {nom} dans Bitrix24')

input_csv = 'Liste_nn_modified.csv'

print("Chargement du fichier CSV local...")
df_local = pd.read_csv(input_csv, low_memory=False)

print(df_local.head())
print(f"Nombre total de lignes dans le fichier local: {len(df_local)}")

df_btx = CrmBitrix(
    content='crm.company.list.json',
    params=[
        'select[]=ID',
        'select[]=TITLE'
    ],
    result="title"
)

update_bitrix_status(df_local, df_btx)

print("Mise à jour des statuts terminée.")


# In[ ]:





# In[62]:


import pandas as pd

file1_path = 'df_filtre_Non_Matching_Siret_YAV_Results.csv'
file2_path = 'listesdesqualiopietnon.csv'

try:
    companyyav_df = pd.read_csv(file1_path)
    all_records_export_df = pd.read_csv(file2_path, delimiter=',', on_bad_lines='skip', low_memory=False)
except pd.errors.ParserError as e:
    print(f"Erreur lors de la lecture des fichiers CSV : {e}")

if 'companyyav_df' in locals() and 'all_records_export_df' in locals():
    print("Colonnes disponibles dans 'listesdesqualiopietnon.csv' :")
    print(all_records_export_df.columns)

    if 'siretetablissementdeclarant' in all_records_export_df.columns:

        companyyav_df['Siret'] = companyyav_df['Siret'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        all_records_export_df['siretetablissementdeclarant'] = all_records_export_df['siretetablissementdeclarant'].astype(str).str.strip()

        missing_siret_in_all_records = companyyav_df[~companyyav_df['Siret'].isin(all_records_export_df['siretetablissementdeclarant'])]

        print(f"Nombre de SIRET dans 'companyyav.csv' qui ne se trouvent pas dans 'listesdesqualiopietnon.csv': {missing_siret_in_all_records.shape[0]}")

        print("Liste des SIRET manquants :")
        print(missing_siret_in_all_records['Siret'].tolist())
    else:
        print("La colonne 'siretetablissementdeclarant' n'existe pas dans le fichier 'listesdesqualiopietnon.csv'.")
else:
    print("Les fichiers n'ont pas pu être chargés correctement.")


# In[63]:


missing_siret_in_all_records.to_csv("Liste_des_entreprises_qui_ont_change_de_statut_et_qui_ne_se_trouve_dans_dgefp.csv", index=False)
missing_siret_in_all_records


# In[64]:


import pandas as pd

file_path = 'Liste_des_entreprises_qui_ont_change_de_statut_et_qui_ne_se_trouve_dans_dgefp.csv'
df = pd.read_csv(file_path)

colonnes_a_conserver = [
    'ID', 'nom', 'Email', 'Crée', 'Mobile', 'Activite1', 'Activite2', 'Activite3', 
    'Adresse', 'CodePostale', 'Ville', 'Siret', 'Statut'
]

df_filtrers = df[colonnes_a_conserver]

df_filtrers['Statut'] = df_filtrers['Statut'].replace('Pas Qualiopi', 'Qualiopi')

df_filtrers.to_csv("Liste_ferme_modified.csv", index=False)
df_filtrers


# In[65]:


import time
import configparser
import requests
import pandas as pd

def CrmBitrix(content='', params=[], result='', pause_duration=1, max_retries=3):
    print("Début de la récupération des données de Bitrix24...")
    
    config = configparser.ConfigParser()
    config.read('config.ini')

    if 'BITRIX' not in config:
        raise ValueError('Section BITRIX manquante dans le fichier \'config.ini\'')

    BITRIX_DOMAIN = config.get('BITRIX', 'BITRIX_DOMAIN')
    BITRIX_TOKEN = config.get('BITRIX', 'BITRIX_TOKEN')

    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    print(f"Utilisation du domaine Bitrix: {BITRIX_DOMAIN}")

    params_str = '&'.join(params)
    BitrixUrl = f'https://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/{content}?{params_str}&start='
    
    listNoms, listIds = [], []

    start = 0
    while True:
        url = BitrixUrl + str(start)
        print(f"Envoi de la requête à l'URL: {url}")
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url)
                response.raise_for_status()
                break
            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    wait_time = pause_duration * (2 ** attempt)
                    print(f'Limite de taux dépassée. Nouvelle tentative dans {wait_time} secondes...')
                    time.sleep(wait_time)
                else:
                    raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')
            except requests.exceptions.RequestException as e:
                raise ValueError(f'Erreur lors de la récupération depuis Bitrix : {e}')

        data = response.json()
        if 'result' in data:
            contacts = data['result']
            print(f"Nombre de contacts récupérés: {len(contacts)}")
            for contact in contacts:
                if result == 'title': 
                    listNoms.append(contact.get('TITLE', ''))
                    listIds.append(contact.get('ID', ''))
                else:
                    return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

        if 'next' in data:
            start = data['next']
        else:
            break

    if result == 'title':
        df_btx = pd.DataFrame({'ID': listIds, 'nom': listNoms})
    else:
        return 'Erreur avec les données attendues. Merci de vérifier la variable \'result\'.'

    print("Récupération des données de Bitrix24 terminée.")
    return df_btx

def get_statut_value(statut):
    statut_mapping = {
        'Pas qualiopi': '140',
        'Qaliopi': '142',
        'Qaliopi à jour': '144',
        'Qaliopi perdu':'146'
    }
    return statut_mapping.get(statut, '146') 

def update_bitrix_status(df_local, df_bitrix):
    print("Début de la mise à jour des statuts dans Bitrix24...")

    config = configparser.ConfigParser()
    config.read('config.ini')

    BITRIX_DOMAIN = config.get('BITRIX', 'BITRIX_DOMAIN')
    BITRIX_TOKEN = config.get('BITRIX', 'BITRIX_TOKEN')

    if not BITRIX_DOMAIN or not BITRIX_TOKEN:
        raise ValueError('Domaine et/ou token Bitrix manquants dans le fichier \'config.ini\'')

    for index, row in df_local.iterrows():
        nom = row['nom']
        statut = row['Statut']
        
        print(f"Vérification du nom: {nom} avec statut: {statut}")
        
        match = df_bitrix[df_bitrix['nom'] == nom]
        if not match.empty:
            bitrix_id = match.iloc[0]['ID']
            statut_value = get_statut_value(statut)
            update_url = f'https://{BITRIX_DOMAIN}/rest/1/{BITRIX_TOKEN}/crm.company.update.json'
            data = {
                'ID': bitrix_id,
                'fields': {
                    'UF_CRM_1722346888': statut_value
                }
            }
            print(f"Envoi de la mise à jour pour l'ID Bitrix: {bitrix_id} avec statut: {statut_value}")
            response = requests.post(update_url, json=data)
            if response.status_code == 200:
                print(f'Statut mis à jour avec succès pour {nom} dans Bitrix24')
            else:
                print(f'Échec de la mise à jour du statut pour {nom} dans Bitrix24')

input_csv = 'Liste_ferme_modified.csv'

print("Chargement du fichier CSV local...")
df_local = pd.read_csv(input_csv, low_memory=False)

print(df_local.head())
print(f"Nombre total de lignes dans le fichier local: {len(df_local)}")

df_btx = CrmBitrix(
    content='crm.company.list.json',
    params=[
        'select[]=ID',
        'select[]=TITLE'
    ],
    result="title"
)

update_bitrix_status(df_local, df_btx)

print("Mise à jour des statuts terminée.")


# In[23]:





# In[ ]:





# In[ ]:




