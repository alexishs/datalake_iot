"""Package métier du data lake IoT.

Code STRICTEMENT identique en debug (dev container) et dans Airflow : les deux
s'exécutent dans le réseau Docker `datalake`, donc le hostname `minio` résout
de la même façon partout. Les DAGs ne font qu'importer et appeler ces fonctions.
"""
