// See https://aka.ms/new-console-template for more information
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;
using System.Runtime.InteropServices;

namespace ParquetFile;


public class Record
{
    public DateTime Timestamp { get; set; }
    public string? EventName { get; set; }
    public double MeterValue { get; set; }

    public override string ToString()
    {
        return $"{Timestamp};{EventName};{MeterValue}";
    }
}

class Program
{
    static async Task Main(string[] args)
    {
        string _tmpBase = "/tmp/";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            _tmpBase = @"c:\temp\";
        }

        string _csvFileName = $"{_tmpBase}/data.csv";
        string _parFileName = $"{_tmpBase}/data.parquet";

        var data = Enumerable.Range(0, 1_000_000).Select(i => new Record
        {
            Timestamp = DateTime.UtcNow.AddSeconds(i),
            EventName = i % 2 == 0 ? "on" : "off",
            MeterValue = i
        }).ToList();

        // Write data to a csv file 
        using (StreamWriter sw = new StreamWriter(_csvFileName))
        {
            foreach (Record r in data)
            {
                Console.WriteLine("Writing: " + r.ToString());
                sw.WriteLine($"{r.Timestamp};{r.EventName};{r.MeterValue}");
            }
        }
        ;

        await ParquetSerializer.SerializeAsync(data, _parFileName);

        IList<Record> readData = ParquetSerializer.DeserializeAsync<Record>(_parFileName).Result;

        foreach (Record r in readData)
        {
            Console.WriteLine("Read: " + r.ToString());
        }

    }
}