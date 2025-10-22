from flask import Flask, jsonify
from cassandra.cluster import Cluster
import os
from dotenv import load_dotenv
from datetime import datetime
from cassandra.auth import PlainTextAuthProvider

app= Flask(__name__)

# load the .env 
load_dotenv()

# Connect to cassandra cluster

auth = PlainTextAuthProvider(username=os.getenv("DB_USERNAME"), password=os.getenv("DB_PASSWORD"))
cluster = Cluster([os.getenv("DB_HOST")],port=os.getenv("DB_PORT"), auth_provider = auth)
session = cluster.connect()

@app.route("/", methods=["GET"])
def home():
    """ home endpoints

    Args :
        None
    
    Return :
        data (json) : body of the api page
    """
    return jsonify("Bienvenue sur la visualisation de données")


@app.route("/retardstotaux", methods=["GET"])
def retardstotaux():
    """ Endpoints to display the sum of delays per day directy from cassandra

    Args :
        None
    
    Return :
        data (json) : body of the api page
    """
    try :
        retards = []
        rows = session.execute("SELECT * FROM sncf.retardstotaux")
        for row in rows:
            retards.append(row)
        return jsonify(retards)
    except :
        return jsonify(f"Une erreur est survenue lors de la lecture de la base SNCF.retardstotaux")

@app.route("/gares/<gare>", methods=["GET"])
def retardsgare(gare):
    """ Endpoints to display the choosen station delays directy from cassandra
    
    Args :
        gare (str) : name of the station

    Returns :
        data (json) : body of the api page
    """
    try :
        if gare != None:
            res = []
            rows = session.execute("SELECT * FROM sncf.retardsgare WHERE gare = %s ALLOW FILTERING", [gare])
            if not rows:
                return jsonify("Gare pas présente dans la bdd")
            else:
                for row in rows:
                    res.append(row)
                return jsonify(res)
        else :
            gares = []
            rows = session.execute("SELECT * FROM sncf.retardsgare")
            if rows:
                for row in rows:
                    gares.append(row)
            return jsonify(gares)
    except :
        return jsonify(f"Une erreur est survenue lors de la lecture de la base sncf.retardsgare")

@app.route("/gares/", methods=["GET"])
def retardsgares():
    """ Endpoints to display the choosen station delays directy from cassandra
    
    Returns :
        data (json) : body of the api page
    """
    try :
        gares = []
        rows = session.execute("SELECT * FROM sncf.retardsgare")
        if rows:
            for row in rows:
                gares.append(row)
        return jsonify(gares)
    except :
        return jsonify(f"Une erreur est survenue lors de la lecture de la base sncf.retardsgare")


if __name__=='__main__':
    app.run(debug=True)