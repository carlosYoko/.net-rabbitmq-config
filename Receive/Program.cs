using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

class Program
{
    static void Main()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using (var connection = factory.CreateConnection())
        {
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "logs", type: ExchangeType.Fanout);

                var queueName = channel.QueueDeclare().QueueName;

                channel.QueueBind(queue: queueName, exchange: "logs", routingKey: "");

                Console.WriteLine("Waiting for logs...");

                var consumer = new EventingBasicConsumer(channel);

                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine($"[x] Received {message}");
                };

                channel.BasicConsume(queue: queueName, autoAck: true, consumer: consumer);
                Console.WriteLine("Press any key to exit");
                Console.ReadLine();
            }
        }
    }
}