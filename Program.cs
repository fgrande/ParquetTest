// See https://aka.ms/new-console-template for more information
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Schema;
using Parquet.Serialization;
using Parquet;
using System.Runtime.InteropServices;
using System.Reflection;

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


public class Config
{
    public string Area { get; set; }
    public string SubArea { get; set; }
    public string Item { get; set; }
    public string Description { get; set; }
    public string Value { get; set; }
}

class Program
{
    static async Task Main(string[] args)
    {
        DateTime _startTime = DateTime.Now;
        TimeSpan _elapsed;

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

        _elapsed = DateTime.Now - _startTime;
        Console.WriteLine($"Data Generated in {_elapsed.TotalSeconds} seconds");

        // Write data to a csv file 
        _startTime = DateTime.Now;
        using (StreamWriter sw = new StreamWriter(_csvFileName))
        {
            foreach (Record r in data)
            {
                //Console.WriteLine("Writing: " + r.ToString());
                sw.WriteLine($"{r.Timestamp};{r.EventName};{r.MeterValue}");
            }
        }
        _elapsed = DateTime.Now - _startTime;
        Console.WriteLine($"Data Written to CSV in {_elapsed.TotalSeconds} seconds");

        _startTime = DateTime.Now;
        await ParquetSerializer.SerializeAsync(data, _parFileName);
        _elapsed = DateTime.Now - _startTime;
        Console.WriteLine($"Data Written to Parquet in {_elapsed.TotalSeconds} seconds");

        _startTime = DateTime.Now;
        IList<Record> readData = ParquetSerializer.DeserializeAsync<Record>(_parFileName).Result;
        _elapsed = DateTime.Now - _startTime;
        Console.WriteLine($"Data Read from Parquet in {_elapsed.TotalSeconds} seconds");



        // Write Parquet file line by line - This is compatible with rel 4.17
        _startTime = DateTime.Now;
        string _parLineFileName = $"{_tmpBase}/data_line.parquet";

        var cfgData = Enumerable.Range(0, 100_000).Select(i => new Config
        {
            Area = $"Area_{i}",
            SubArea = $"SubArea_{i}",
            Item = $"Item_{i}",
            Description = $"Description_{i}",
            Value = $"Value_{i}"
        }).ToList();

        List<DataField> _fields = new List<DataField>(){
                new DataField("Area", Type.GetType("System.String")),
                new DataField<string>("SubArea"),
                new DataField<string>("Item"),
                new DataField<string>("Description"),
                new DataField<string>("Value"),
            };

        int _batchSize = 12983;


        using (Stream _stream = System.IO.File.Create(_parLineFileName))
        {

            ParquetSchema _ps = new ParquetSchema(_fields);

            List<Config> _batchRows;

            bool _append = false;

            int _batch = 0;
            bool _goOn = true;
            while (_goOn)
            {
                int _start = _batch * _batchSize;
                int _end = _start + _batchSize;

                _batchRows = cfgData.Skip(_start).Take(_batchSize).ToList();
                if (_batchRows.Count == 0)
                {
                    break;
                }

                Console.WriteLine($"Writing batch {_batch} : {_start} to {_end} => {_batchRows.Count} rows");

                using (var _pWriter = await ParquetWriter.CreateAsync(_ps, _stream, null, _append))
                {
                    _append = true;
                    using (ParquetRowGroupWriter _rgWriter = _pWriter.CreateRowGroup())
                    {
                        DataColumn _dc;

                        foreach (DataField _fld in _fields)
                        {
                            switch (_fld.ClrType.ToString())
                            {
                                case "System.String":
                                    _dc = new DataColumn(_fld, _batchRows.Select(x => x.GetType().GetProperty(_fld.Name).GetValue(x)?.ToString()).ToArray());
                                    break;
                                default:
                                    throw new Exception($"Manage Type {_fld.ClrType}");
                            }

                            await _rgWriter.WriteColumnAsync(_dc);
                        }

                        _batch++;
                    }
                }
            }
        }

        _elapsed = DateTime.Now - _startTime;
        Console.WriteLine($"Data Written line-by-line to Parquet in {_elapsed.TotalSeconds} seconds");
    }
}