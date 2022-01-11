from couchbase.cluster import Cluster, ClusterOptions
from couchbase_core.cluster import PasswordAuthenticator
import json

maxRetries = 2

def create_record(maxRetries):
    try:
        cluster = Cluster('couchbase://localhost', ClusterOptions(PasswordAuthenticator('Administrator', 'password')))
        cb = cluster.bucket('beer-sample')
        cb_coll = cb.default_collection()

        from couchbase.cluster import QueryOptions
        import json
        #row_iter = cluster.query('SELECT name FROM `customers` where $1 IN interests', QueryOptions(positional_parameters=['African XYZ']))

        print("Result using a query...   ")

        row_iter = cluster.query('SELECT * FROM `beer-sample` where meta().id = "21st_amendment_brewery_cafe"')
        for row in row_iter: print(row)

        print("   ")

        result = cb_coll.get("21st_amendment_brewery_cafe")
        content = result.content
        print("Type of content: {}".format(type(content)))

        print("Result using KV")
        print(json.dumps(content))

    except couchbase.exceptions.CouchbaseError as err:
            # isRetryable will be true for transient errors, such as a CAS mismatch (indicating
            # another agent concurrently modified the document), or a temporary failure (indicating
            # the server is temporarily unavailable or overloaded).  The operation may or may not
            # have been written, but since it is idempotent we can simply retry it.
            if err.is_retryable:
                if maxRetries > 0:
                    print("Retrying operation on retryable err " + err)
                    create_record(maxRetries - 1)
                else:
                    # Errors can be transient but still exceed our SLA.
                    print("Too many attempts, aborting on err {0}".format(err))
                    raise

            # If the err is not isRetryable, there is perhaps a more permanent or serious error,
            # such as a network failure.
            else:
                print("Aborting operation on err {0}".format(err))
                raise

create_record(maxRetries)