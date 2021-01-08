import pymongo
import os
import subprocess

from tkinter import filedialog
from pprint import pprint

try:
    from ids import user, passw, db
except:
    user = 'stephane_isen'
    passw = 'isenBrest_29'

# Règle un problème avec la fenetre tkinter sur Ubuntu 20.04
if os.environ.get('DISPLAY','') == '':
    os.environ.__setitem__('DISPLAY', ':0.0')

# Classe de la connection à la base de données
class MongoDB_Log():

    # Méthode pour se connecter à la bdd
    @classmethod
    def login(cls, user=user, passw=passw, db=None):
        return pymongo.MongoClient(f"mongodb+srv://{user}:{passw}@clusterkata.b6v13.mongodb.net/{db}?retryWrites=true&w=majority")

    # Méthode pour ouvrir la connection à la bdd
    @classmethod
    def open_con(cls):
        cls.client = cls.login()
        cls.db = cls.client.dblp
        cls.collect = cls.db.publis

    # Méthode pour fermer la connection à la bdd
    @classmethod
    def close_con(cls):
        cls.client.close()

    # Méthode pour compter le nombre de documents de la collection
    @classmethod
    def count_docs(cls):
        cls.open_con()
        data = list(cls.collect.find())
        cls.close_con()
        return print(f"Il y a {len(data)} documents dans notre base de données.")

    # Méthode pour lister tous les livres de type "Book"
    @classmethod
    def list_type(cls):
        cls.open_con()
        data = list(cls.collect.find({'type':'Book'}))
        cls.close_con()
        return pprint(data)

    # Méthode pour lister tous les livres depuis 2014
    @classmethod
    def list_book_date(cls, date):
        cls.open_con()
        data = list(cls.collect.find({'type':'Book', 'year':{'$gte':date}}, {'_id':0, 'title':1, 'authors':1}))
        cls.close_con()
        return pprint(data)

    # Méthode pour lister les publications en fonction d'un auteur (ici Toru Ishida)
    @classmethod
    def list_publi_auth(cls, auth):
        cls.open_con()
        data = list(cls.collect.find({'authors':auth}, {'_id':0, 'title':1, 'authors':1}))
        cls.close_con()
        return pprint(data)

    # Méthode pour lister tous les auteurs de la bdd
    @classmethod
    def list_all_auth(cls):
        cls.open_con()
        data = list(cls.collect.distinct("authors"))
        cls.close_con()
        return pprint(data)

    # Méthode pour trier les publications d'un auteur en fonction du titre
    @classmethod
    def list_publi_auth_sort(cls, auth):
        cls.open_con()
        data = list(cls.collect.find({"authors":auth}).sort('title',1))
        cls.close_con()
        return pprint(data)

    # Méthode pour compter le nombre de publications d'un auteur
    @classmethod
    def count_publi_auth(cls, auth):
        cls.open_con()
        data = list(cls.collect.find({"authors":auth}))
        cls.close_con()
        return print(f"Il y a {len(data)} publications de {auth}.")

    # Méthode pour compter le nombre de publications depuis 2011 et par type
    @classmethod
    def count_publi_date_type(cls, date):
        cls.open_con()
        data = list(cls.collect.aggregate([{'$match':{"year":{'$gte':date}}}, {'$group':{'_id':'$type','count':{'$sum':1}}}]))
        cls.close_con()
        return pprint(data)

    # Méthode pour compter le nombre de publications par auteur et trier par ordre croissant
    @classmethod
    def count_publi_all_auth(cls):
        cls.open_con()
        data = list(cls.collect.aggregate([{'$unwind':'$authors'}, {'$group' : { '_id' : '$authors', 'count' : {'$sum' : 1}}}, {'$sort':{'count':1}}]))
        cls.close_con()
        return pprint(data)

    # Méthode pour choisir le fichier json à insérer
    @classmethod
    def insert_json(cls, collect_name):
        cls.open_con()
        file = filedialog.askopenfilename()
        name = file.split('/')[-1]
        subprocess.call(f"mongoimport --uri mongodb+srv://{user}:{passw}@clusterkata.b6v13.mongodb.net/{db} --collection {collect_name} --type json --file {file}", shell=True)
        cls.close_con()
        return print(f"Le fichier {name} a bien été inséré dans la collection {collect_name}.")

'''
Toutes les commandes du brief.
Voir le résultat dans le notebook de présentation.
'''
# MongoDB_Log().count_docs()
# MongoDB_Log().list_type()
# MongoDB_Log().list_book_date(2014)
# MongoDB_Log().list_publi_auth("Toru Ishida")
# MongoDB_Log().list_all_auth()
# MongoDB_Log().list_publi_auth_sort("Toru Ishida")
# MongoDB_Log().count_publi_auth("Toru Ishida")
# MongoDB_Log().count_publi_date_type(2011)
# MongoDB_Log().count_publi_all_auth()
# MongoDB_Log.insert_json('nom_de_la_collection')