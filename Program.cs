using System;
using System.Collections.Generic;
using Couchbase;
using Couchbase.Configuration.Client;
using Couchbase.Authentication;

namespace sdk2code
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World! 2.x");

           /* var cluster = new Cluster(new ClientConfiguration
            {
                Servers = new List<Uri> { new Uri("http://127.0.0.1")}
            });

            var authenticator = new PasswordAuthenticator("Administrator",
                "password");

            cluster.Authenticate(authenticator);

            var bucket = cluster.OpenBucket("demo"); */

            ClusterHelper.Initialize(new ClientConfiguration
            {
                Servers = new List<Uri> { new Uri("http://127.0.0.1")}
            }, new PasswordAuthenticator("Administrator", "password"));
            var bucket = ClusterHelper.GetBucket("demo");

            var fullKey = "dotnet2x";
            var item = new Document<dynamic>
            {
                Id = fullKey,
                Content = new
                {
                    name = "Couchbase"
                }
            };

            var upsert = bucket.Upsert(item);

            Console.WriteLine("Hit Any Key to continue...");
            Console.ReadKey();
            
            //var result = await collection.GetAsync("string-key");
            //var content = result.ContentAs<String>();

            //Console.WriteLine("Without Expiry Set = " + result.Document.Expiry);

            
            
        }

        
    }
}
