using System;
using Cassandra;

namespace CSharpBencher
{
    class Program
    {
        // General settings
        private static readonly string[] _contactPoints = { "<INSERT_YOUR_CONTACT_HOST_HERE>" };
        private const string _localDataCenter = "<INSERT_YOUR_DC_HERE>";
        
        // Write settings
        private const int _seriesToInsert = 100;
        private const int _writeParallelStatementsCount = 64;
        
        // Read settings
        private const int _readParallelStatementsCount = 30;
        private const int _readPageSize = int.MaxValue;

        static void Main()
        {
            Console.WriteLine("Press:");
            Console.WriteLine("-\"r\" for read benchmark");
            Console.WriteLine("-\"w\" for write (ParallelPersistor)");
            Console.WriteLine("-\"x\" for write (SemaphorePersistor)");
            Console.WriteLine("-\"c\" for cleanup");

            var readKey = Console.ReadKey().KeyChar;
            Console.WriteLine();

            switch (readKey)
            {
                case 'c':
                    TruncateTables(_contactPoints, _localDataCenter);
                    Console.WriteLine("Tables truncated");
                    break;
                case 'r':
                    Console.WriteLine($"Starting read benchmark, processing {_readParallelStatementsCount} statements in parallel, with pages of size {_readPageSize} on DC {_localDataCenter}");
                    var reader = Reader.CreateAndConnect(_contactPoints, _localDataCenter);
                    reader.Read(_readParallelStatementsCount, _readPageSize);
                    break;
                case 'w':
                    Console.WriteLine($"Starting write benchmark with {_seriesToInsert} series, sending {_writeParallelStatementsCount} statements in parallel, on DC {_localDataCenter}");
                    var writer1 = Writer.CreateAndConnect(_contactPoints, _localDataCenter);
                    writer1.Write(_seriesToInsert, new ParallelPersistorStrategy(_writeParallelStatementsCount));
                    break;
                case 'x':
                    Console.WriteLine($"Starting write benchmark with {_seriesToInsert} series, sending {_writeParallelStatementsCount} statements in parallel, on DC {_localDataCenter}");
                    var writer2 = Writer.CreateAndConnect(_contactPoints, _localDataCenter);
                    writer2.Write(_seriesToInsert, new SemaphorePersistorStrategy(_writeParallelStatementsCount));
                    break;
            }
        }

        private static void TruncateTables(string[] cassandraContactPoints, string localDc)
        {
            var session = Cluster.Builder()
                  .WithDefaultKeyspace("CsharpDriverBenchmark")
                  .WithQueryTimeout((int)TimeSpan.FromSeconds(10).TotalMilliseconds)
                  .AddContactPoints(cassandraContactPoints)
                  .WithLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy(localDc)))
                  .Build()
                  .Connect();

            session.Execute("TRUNCATE \"CsharpDriverBenchmark\".\"SerieId\";");
            session.Execute("TRUNCATE \"CsharpDriverBenchmark\".\"Timeserie\";");
            session.Dispose();
        }
    }
}