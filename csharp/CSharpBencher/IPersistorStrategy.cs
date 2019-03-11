using System.Collections.Generic;
using Cassandra;

namespace CSharpBencher
{
    public interface IPersistorStrategy
    {
        int PersistedPointCount { get; }

        void Prepare(ISession session);
        void Run(ISession session, IEnumerable<DataToInsert> generatedData, int totalPointCount);
        void Cleanup();
    }
}
