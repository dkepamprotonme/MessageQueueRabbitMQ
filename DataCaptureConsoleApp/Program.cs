using Common;
using RabbitMQ.Client;
using System.Text.Json;

using System.Text;

namespace DataCaptureConsoleApp
{
    public class Program
    {
        public static readonly string InputDirectory = "../../../../Input";
        public static readonly string InputFileExtensionSearchPattern = "*.avi";
        private static void Main()
        {
            var factory = new ConnectionFactory() { HostName = Configuration.HostName };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDeclare(queue: Configuration.QueueName,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);
            var directoryPath = Path.GetFullPath(InputDirectory);
            var files = Directory.EnumerateFiles(directoryPath, InputFileExtensionSearchPattern);
            foreach (var filePath in files)
            {
                try
                {
                    var fileName = Path.GetFileName(filePath);
                    byte[] buffer = new byte[Configuration.PartByte];
                    int bytesRead;
                    int part = 0;
                    using var fs = File.Open(filePath, FileMode.Open, FileAccess.Read);
                    using var bs = new BufferedStream(fs);
                    while ((bytesRead = bs.Read(buffer, 0, Configuration.PartByte)) != 0)
                    {
                        part++;
                        var dataModel = new DataModel() { FileName = fileName, Data = buffer };
                        string message = JsonSerializer.Serialize(dataModel);
                        var body = Encoding.UTF8.GetBytes(message);
                        channel.BasicPublish(exchange: "",
                                             routingKey: Configuration.QueueName,
                                             basicProperties: null,
                                             body: body);
                    }
                }
                catch
                {
                }
            }
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}