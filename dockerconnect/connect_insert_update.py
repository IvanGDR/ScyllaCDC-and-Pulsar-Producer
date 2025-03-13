from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from ssl import SSLContext, PROTOCOL_TLS, CERT_REQUIRED

import random
import time

### Connect to the ScyllaDB cluster
contact_points = ['172.19.0.2']  # Replace with your actual service name or IPs


### Create a cluster instance with RBAC only
#username = 'your_username' # if using rbac
#password = 'your_password' # if using rbac
#auth_provider = PlainTextAuthProvider(username, password)
#cluster = Cluster(contact_points, auth_provider=auth_provider)


# connect to cluster
cluster = Cluster(contact_points)
session = cluster.connect()


# Create a Keyspace

session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS ivan_cdc_ks
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}
    AND durable_writes = true;
    """)



# Set Keyspace
session.set_keyspace('ivan_cdc_ks')

# Truncate Table in case it exists
truncate_query = "TRUNCATE ivan_cdc_table"
truncate_cdc_query = "TRUNCATE ivan_cdc_table_scylla_cdc_log"
session.execute(truncate_query)
session.execute(truncate_cdc_query)

# Create table
session.execute(
    """
    CREATE TABLE IF NOT EXISTS ivan_cdc_ks.ivan_cdc_table (
    sensor_id text,
    coordinate int,
    status text,
    PRIMARY KEY(sensor_id, coordinate))
    WITH cdc = {'enabled': true};
    """)


#### INSERT QUERY
iter = 3000

print("Inserting Records.....")
for i in range(iter):
    # Prepare the INSERT statement
    query="INSERT INTO ivan_cdc_table (sensor_id, coordinate, status) VALUES (?, ?, ?)"
    prepstmt = session.prepare(query)
    session.execute(prepstmt,('sensor'+str(i), i, 'ACTIVE'))
    time.sleep(1) # in seconds

print("Code Executed")
