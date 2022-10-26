using Common;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.IO;
using System.Text;
using System.Text.Json;
namespace MainProcessingConsoleApp
{
    public class Program
    {
        public static readonly string LogDirectory = "../../../../Logs";
        public static readonly string LogFileExtension = ".txt";
        public static readonly string OutputDirectory = "../../../../Output";
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
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                var dataModel = JsonSerializer.Deserialize<DataModel>(message);
                if (dataModel == null || dataModel.FileName == null || dataModel.Data == null)
                {
                    return;
                }
                var logDirectoryPath = Path.GetFullPath(LogDirectory);
                Directory.CreateDirectory(logDirectoryPath);
                var logFileName = DateTime.Now.ToFileTime().ToString() + LogFileExtension;
                var logFilePath = Path.Combine(logDirectoryPath, logFileName);
                File.AppendAllText(logFilePath, message);
                var outputDirectoryPath = Path.GetFullPath(OutputDirectory);
                Directory.CreateDirectory(outputDirectoryPath);
                var outputFileName = dataModel.FileName;
                var outputFilePath = Path.Combine(outputDirectoryPath, outputFileName);
                using var stream = new FileStream(outputFilePath, FileMode.Append);
                stream.Write(dataModel.Data);
            };
            channel.BasicConsume(queue: Configuration.QueueName,
                                 autoAck: true,
                                 consumer: consumer);
            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}