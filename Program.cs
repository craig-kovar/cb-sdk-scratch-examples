using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Newtonsoft;
using Couchbase;
using Couchbase.KeyValue;
using System.Text;
using Couchbase.Core.IO.Transcoders;
using Couchbase.Transactions;
using Couchbase.Transactions.Config;
using Couchbase.Transactions.Deferred;
using Couchbase.Transactions.Error;
using Couchbase.Core.Retry;
using Couchbase.Core.Exceptions;
using Couchbase.Core.Exceptions.KeyValue;
using Couchbase.Query;
using System.Security.Cryptography;

namespace sdk2to3
{

    class Program
    {
        static UpsertOptions options = new UpsertOptions().Timeout(TimeSpan.FromSeconds(5));

        static async Task Main(string[] args)
        {
            Console.WriteLine("Starting 3.X Demo");

            var cluster = await Cluster.ConnectAsync("couchbase://localhost", 
                "Administrator", "password");
            var bucket = await cluster.BucketAsync("demo");
    
            await bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(10));
            var collection = bucket.DefaultCollection();

            //await DoArrayTest(collection);

            await DoRetryTest(cluster);

            //for (int i = 0; i < 10; i++) {
            //    await DoError(collection);
            //}
            
        }

        public static async Task DoError(ICouchbaseCollection collection) {
            await collection.ExistsAsync("failkey");
        }
        public static async Task DoArrayTest(ICouchbaseCollection collection) {
            //Sample Code to create a simple JSON Document we will add/manipulate array in
            //await collection.InsertAsync("test1",new Poco("test1","test1@test.com"));
            try {
                //Using the syntax to perform multiple subdoc operations on a single
                //document as a single operation
                await collection.MutateInAsync("test1", specs => {
                    //Add an item to the beginning of an array
                    //Create the path if it does not already exist
                    specs.ArrayPrepend("myPreArrays",
                        new [] {new Poco("array1","array1@email.com")}, 
                        createParents: true);

                    //Add an item to the end of an array
                    //Create the path if it does not already exist
                    specs.ArrayAppend("myAppArrays",
                        new [] {new Poco("array1","array1@email.com")}, 
                        createPath: true);

                    //We can not use ArrayAddUnique as it has the following limitation
                    //per: https://docs.couchbase.com/dotnet-sdk/current/howtos/subdocument-operations.html#arrays-as-unique-sets
                    //If the array contains JSON Floats, Objects, or Arrays will fail with either a
                    //PathMismatchException or CannotInsertValueException depending on
                    //the values in the array and/or the value being added

                    //We will insert an object to an existing array
                    specs.ArrayInsert("myArray[3]",
                           new [] {new Poco("insert","insert@email.com")});

                    specs.Replace("myArray[1]",new Poco("finally","email"));
                });
            } catch (PathNotFoundException err) {
                //Catch a PathNotFoundException if path not in document
                //and createParents is false
                Console.WriteLine("Missing path : " + err.Message);
            } catch (InvalidArgumentException inv) {
                //Catch an InvalidArgumentException if the ArrayInsert tries
                //to insert into an array that does not exist or the value is out of bounds
                Console.WriteLine("Invalid Argument : " + inv.Message);
            }
        }  

        public static async Task DoRetryTest(ICluster cluster) {
            //Custom defined Retry Strategy to throw IndexNotFoundException
            //if the prepared statement is missing
            MyRetry retry = new MyRetry();
            
            //Query Options to use custom retry strategy and auto_execute the
            //statement after it is prepared
            QueryOptions options = new QueryOptions();
            options.RetryStrategy(retry);
            options.Raw("auto_execute",true);

            String name = "test";
            String statement = "select * from `travel-sample` where type=$type and id = $id";
            
            Dictionary<string, object> myParams = new Dictionary<string, object>()
            {
                { "type", "landmark" },
                { "id", 10019}
            };


            try {
                await DoQuery(cluster,name, statement, myParams, options);
            } catch (CouchbaseException ex) {
                Console.WriteLine(ex);
            }

            await cluster.DisposeAsync();
            
        }

        // Simple SHA1 hashing method
        static string Hash(string input)
        {
            using (SHA1Managed sha1 = new SHA1Managed())
            {
                var hash = sha1.ComputeHash(Encoding.UTF8.GetBytes(input));
                var sb = new StringBuilder(hash.Length * 2);

                foreach (byte b in hash)
                {
                    // can be "x2" if you want lowercase
                    sb.Append(b.ToString("X2"));
                }

                return sb.ToString();
            }
        }

        public static async Task DoQuery(ICluster cluster, String name,
        String statement, Dictionary<String,Object> myParams, QueryOptions options) {

            //Internal statements used for this generic method
            string executeStatement = "execute {0}";
            string prepareStatement = "prepare {0} as {1}";
            string deleteStatement = "delete from system:prepareds where name = \"{0}\"";

            // Format the prepared statement name using SHA1 hash of statement
            // This is done to detect any changes to the query statement automatically
            string formatted_name = name + "_" + Hash(statement);

            Console.WriteLine("Starting query test"); 
            decimal startTime =  DateTime.Now.Ticks / (decimal)TimeSpan.TicksPerMillisecond;

            //Query results
            IQueryResult<dynamic> resultDynamic = null;
                    
            //Build Parameters
            foreach(var item in myParams)
            {
                options.Parameter(item.Key,item.Value);
            }

            try {
                resultDynamic = await cluster.QueryAsync<dynamic>(
                    String.Format(executeStatement,formatted_name), options
                );
            } catch (IndexNotFoundException ex) {
                //Write your own logging message here
                Console.WriteLine("Got into catch block " + ex.GetType());

                //Try deleting prepared statement just in case
                resultDynamic = await cluster.QueryAsync<dynamic>(
                    String.Format(deleteStatement,formatted_name), options
                );

                //Prepare and auto_execute the statement
                resultDynamic = await cluster.QueryAsync<dynamic>(
                    String.Format(prepareStatement,formatted_name,statement),
                    options
                );
            }

            //Print out the results - custom logic can go here can return for further
            //processing
            IAsyncEnumerable<dynamic> dynamicRows = resultDynamic.Rows;
            await foreach (var row in dynamicRows)
            {
                Console.WriteLine(row);
            }

            decimal stopTime = DateTime.Now.Ticks / (decimal) TimeSpan.TicksPerMillisecond;
            decimal diff = stopTime - startTime;
            Console.WriteLine("Query test took " + diff + " ms");
        }

    
    }

    
}
