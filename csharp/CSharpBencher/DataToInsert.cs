using System;

namespace CSharpBencher
{
    public struct DataToInsert
    {
        public const string InsertStatement = @"INSERT INTO ""Timeserie"" (""SerieId"", ""Day"", ""UtcDate"", ""Value"") VALUES (?, ?, ?, ?) USING TTL ?;";
        public static readonly int TimeToLive = (int)TimeSpan.FromDays(8).TotalSeconds;

        public Guid SerieId;
        public DateTime Timestamp;
        public double Value;

        public DataToInsert(Guid serieId, DateTime timestamp, double value)
        {
            SerieId = serieId;
            Timestamp = timestamp;
            Value = value;
        }
    }
}