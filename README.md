# TP Big Data - Streaming Pipeline Vélib

## Le Sujet
Ce projet a pour objectif de construire un pipeline de traitement de données en temps réel (Streaming) en utilisant les technologies de l'écosystème Big Data.
L'application récupère en continu les données en direct de l'API Open Data de Paris concernant la disponibilité des vélos en libre-service (Vélib). Ces données brutes sont ingérées, transportées puis traitées en mémoire pour calculer des statistiques en direct (comme le taux de remplissage ponctuel des stations).

## L'Architecture
Le projet repose sur une architecture distribuée conteneurisée, gérée par Docker Compose :
1. **API Open Data (Source) :** L'API officielle de la ville de Paris fournissant l'état des stations Vélib.
2. **Producteur Python (Ingestion) :** Le script `src/fetch_data.py` interroge l'API toutes les 30 secondes et agit comme un *Kafka Producer*. Il récupère, convertit les données, et les pousse vers le broker Kafka.
3. **Apache Kafka (Message Broker) :** Centralise le flux de données en temps réel. Il reçoit les messages JSON sur le topic `velib-stations` et joue le rôle de tampon haute performance. (Configuré avec KRaft et des listeners réseau internes/externes).
4. **Apache Spark (Stream Processing) :** Un mini-cluster Spark (Master + Worker) exécute le script PySpark `spark_jobs/process_velib.py`. Il écoute le flux Kafka en continu (*Spark Structured Streaming*), parse les objets JSON, filtre les stations hors service, et calcule à la volée le taux de disponibilité des vélos (`fill_percentage`).

---

## Instructions de Lancement

### 1. Prérequis
- Avoir [Docker Desktop](https://www.docker.com/) allumé et fonctionnel.
- Avoir Python 3 installé sur votre machine avec `pip`.

### 2. Démarrer l'infrastructure
Montez les conteneurs (Kafka, Spark Master, Spark Worker...) en tâche de fond :
```bash
docker compose up -d
```
> **Note (Utilisateurs Windows) :** En cas d'erreur réseau de type `connectex` empêchant le téléchargement des images, relancez Docker Desktop ou réinitialisez le composant réseau Windows (`net stop winnat` puis `net start winnat` dans une invite de commande administrateur).

### 3. Installer les dépendances Python
Installez localement les paquets requis pour le producteur :
```bash
pip install -r requirements.txt
```

### 4. Lancer le Flux d'Ingestion (Producteur)
Dans votre premier terminal, exécutez le script qui récupère les données et les envoie dans Kafka :
```bash
python src/fetch_data.py
```
*(Ce script tourne en boucle, laissez ce terminal ouvert).*

### 5. Démarrer l'Analyse en Temps Réel (Spark)
Ouvrez un **nouveau terminal** (pour laisser tourner le premier) et soumettez le job de streaming au serveur Spark :
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf "spark.jars.ivy=/tmp/.ivy2" \
  /opt/spark/jobs/process_velib.py
```
Dès que Spark est initialisé, vous verrez apparaître et se rafraîchir en direct, dans cette console, de magnifiques tableaux incluant les capacités et pourcentages de remplissage de vos stations Vélib !
