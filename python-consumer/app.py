from kafka import KafkaConsumer
import sys
from cassandra.cluster import Cluster
import json
from datetime import datetime
from dotenv import load_dotenv
import os
from cassandra.auth import PlainTextAuthProvider
from prometheus_client import start_http_server, Gauge
import time, threading

object_counter_gare = Gauge('object_counter_gare_consumer', "Amount of train stations proceed during the last 30 seconds")
current_obj_count_gare = 0
object_counter_pert = Gauge('object_counter_pert_consumer', "Amount of perturbations proceed during the last 30 seconds")
current_obj_count_pert = 0

tester = dict()

def resetCounter():
    global current_obj_count_gare, current_obj_count_pert
    # update the metrics
    object_counter_gare.set(current_obj_count_gare)
    object_counter_pert.set(current_obj_count_pert)

    # reset obj count
    current_obj_count_gare = 0
    current_obj_count_pert = 0

class setInterval :
    def __init__(self,interval,action) :
        self.interval=interval
        self.action=action
        self.stopEvent=threading.Event()
        thread=threading.Thread(target=self.__setInterval)
        thread.start()

    def __setInterval(self) :
        nextTime=time.time()+self.interval
        while not self.stopEvent.wait(nextTime-time.time()) :
            nextTime+=self.interval
            self.action()

    def cancel(self) :
        self.stopEvent.set()

def processSignificantDelays(msg):
    global current_obj_count_gare, current_obj_count_pert, tester, benchmark_enable

    try :
        # update obj count pert
        current_obj_count_pert += 1

        if benchmark_enable == 0:
            disruptionId = msg.value["disruption_id"]
            if (disruptionId in tester):
                tester[disruptionId] += 1 
                print("DOUBLON (", disruptionId, ") =>", tester[disruptionId])
            else:
                tester[disruptionId] = 1

        datefinpert = msg.value["application_periods"][0]["end"]
        dernierarret = None
        datepert = datetime.strptime(datefinpert, '%Y%m%dT%H%M%S').date() # Date de la perturbation

        # Traitement par gare
        for gare in msg.value["impacted_objects"][0]["impacted_stops"]: 
            try :
                # update obj count gare
                current_obj_count_gare += 1

                if ("arrival_status" not in gare or gare["arrival_status"] != "delayed"):
                    continue

                dernierarret = gare

                hreellegare = datetime.strptime(gare["amended_arrival_time"], '%H%M%S') # Heure d'arrivée réelle
                hprevuegare = datetime.strptime(gare["base_arrival_time"], '%H%M%S') # Heure d'arrivée prévue
                nomgare = gare["stop_point"]["name"] # Nom de la gare concernée

                # On donne le retard total en secondes
                retardgare = int((hreellegare-hprevuegare).total_seconds())

                if retardgare < 0: # Si le train doit arriver à 23h mais qu'il arrive à 1h par exemple
                    retardgare += 24 * 3600

                requestgare = session.execute("SELECT retardtotal FROM sncf.retardsgare WHERE date = %s AND gare = %s;", [datepert, nomgare])

                if requestgare.one() is None: 
                    if benchmark_enable == 0:
                        print(retardgare, " (", nomgare, ")")
                    
                    # Alors on insert une nouvelle valeur avec la date et le retard en secondes
                    session.execute("INSERT INTO sncf.retardsgare (date, retardtotal, gare) VALUES(%s, %s, %s);", [datepert, retardgare, nomgare])
                else: 
                    # Sinon on prend le retard courant + le retard indiqué dans la base et on update la valeur
                    updategare = requestgare.one().retardtotal + retardgare
                    
                    if benchmark_enable == 0:
                        print(retardgare, " (", nomgare, ") =>", updategare)
                    
                    session.execute("UPDATE sncf.retardsgare SET retardtotal = %s WHERE date = %s AND gare = %s;", [updategare, datepert, nomgare])
            except Exception as e:
                print("Une erreur de traitement de gare a eu lieu")
                print(e)
            
        # Traitement global des retards
        # Conversion des horaires en objets datetime
        if dernierarret is None:
            return

        hreelle = datetime.strptime(dernierarret["amended_arrival_time"], '%H%M%S') # Heure d'arrivée réelle
        hprevue = datetime.strptime(dernierarret["base_arrival_time"], '%H%M%S') # Heure d'arrivée prévue

        # On donne le retard total en secondes
        retardtotal = int((hreelle-hprevue).total_seconds())

        if retardtotal < 0: # Si le train doit arriver à 23h mais qu'il arrive à 1h par exemple
            retardtotal += 24 * 3600

        # Check si une valeur existe pour la date de la perturbation
        request = session.execute("SELECT retardtotal FROM sncf.retardstotaux WHERE date = %s;", [datepert])

        if request.one() is None: 
            # Alors on insert une nouvelle valeur avec la date et le retard en secondes
            session.execute("INSERT INTO sncf.retardstotaux (date, retardtotal) VALUES(%s, %s);", [datepert, retardtotal])
        else: 
            # Sinon on prend le retard courant + le retard indiqué dans la base et on update la valeur
            update = request.one().retardtotal + retardtotal
            session.execute("UPDATE sncf.retardstotaux SET retardtotal = %s WHERE date = %s;", [update, datepert])
    except Exception as e:
        print("Une erreur de traitement a eu lieu")
        print(e)

# Charge les variables d'environnement
load_dotenv()

cassandrausername=os.getenv("DB_USERNAME")
cassandrapassword=os.getenv("DB_PASSWORD")
cassandrahost = os.getenv("DB_HOST", "127.0.0.1")
cassandraport = int(os.getenv("DB_PORT", 9042))
kafkahost = os.getenv("KAFKA_HOST", "localhost")
kafkaport = os.getenv("KAFKA_PORT", 9092)
kafkatopic = os.getenv("KAFKA_TOPIC", "disruption")
benchmark_enable = int(os.getenv("BENCHMARK_ENABLE", 0))

# Connexion au serveur Kafka
kafkaserver = kafkahost + ":" + kafkaport
bootstrap_servers = [kafkaserver]
consumer = KafkaConsumer(kafkatopic,
                         group_id='group1',
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
print("> Consommateur Kafka créé")

# Connexion Cassandra
auth = PlainTextAuthProvider(username=cassandrausername, password=cassandrapassword)
cluster = Cluster([cassandrahost], port=cassandraport, auth_provider=auth)
session = cluster.connect()
print("> Connecté à la base de données")

# Création de la Base SNCF et des tables retardstotaux et retardsgare
session.execute("CREATE KEYSPACE IF NOT EXISTS SNCF WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
session.execute("CREATE TABLE IF NOT EXISTS SNCF.retardstotaux ( date timestamp PRIMARY KEY, retardtotal int);")
session.execute("CREATE TABLE IF NOT EXISTS SNCF.retardsgare ( date timestamp, retardtotal int, gare text, PRIMARY KEY(date, gare));")

# Lancement de l'API Prometheus
setInterval(30, resetCounter)

start_http_server(5000)
print("> API Prometheus démarée sur le port 5000")

# Traitement du JSON
print("> Début du traitement des données")
for msg in consumer:
    if msg.value["status"] == "past":
        if msg.value["severity"]["effect"] == "SIGNIFICANT_DELAYS":
            processSignificantDelays(msg)