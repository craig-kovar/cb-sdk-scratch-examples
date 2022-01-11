from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator

cluster = Cluster('couchbase://localhost')
authenticator = PasswordAuthenticator('Administrator', 'password')
cluster.authenticate(authenticator)
cb = cluster.open_bucket('demo')

