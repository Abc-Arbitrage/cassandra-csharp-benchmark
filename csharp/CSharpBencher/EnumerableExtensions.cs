using System.Collections.Generic;

namespace CSharpBencher
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IReadOnlyCollection<T>> Partition<T>(this IEnumerable<T> @this, int partitionSize)
        {
            return InnerPartition(@this, partitionSize);
        }

        private static IEnumerable<IReadOnlyCollection<T>> InnerPartition<T>(this IEnumerable<T> @this, int partitionSize)
        {
            var partition = new List<T>(partitionSize);
            foreach (var row in @this)
            {
                partition.Add(row);
                if (partition.Count != partitionSize)
                    continue;

                yield return partition;
                partition = new List<T>(partitionSize);
            }
            if (partition.Count != 0)
                yield return partition;
        }
    }
}