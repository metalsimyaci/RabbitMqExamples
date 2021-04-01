using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;

namespace RabbitMqExamples.Console
{
    class Program
    {
        private const string QUEUE_NAME = "BiQueue";
        private const string RABBITMQ_HOST = "localhost";
        private const string FANOUT_EXCHANGE_NAME = "Fanout_Exchange";
        private const string DIRECT_EXCHANGE_NAME = "Direct_Exchange";
        private const string TOPIC_EXCHANGE_NAME = "Topic_Exchange";
        private const string HEADER_EXCHANGE_NAME = "Header_Exchange";

        static void Main(string[] args)
        {

            try
            {
                SmartQueuePublisher();
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e.Message);
            }
            finally
            {
                System.Console.Read();
            }
        }

       
        private static void BasicPublisher()
        {
            ConnectionFactory factory = new ConnectionFactory {HostName = RABBITMQ_HOST};

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.QueueDeclare(QUEUE_NAME, false, false, true);

            var message = "bi test mesajı";
            byte[] byteMessage = Encoding.UTF8.GetBytes(message);
            channel.BasicPublish(exchange: "", routingKey: QUEUE_NAME, body: byteMessage);
            System.Console.WriteLine("Message published Success!");
        }

        private static void SmartQueuePublisher()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.QueueDeclare(QUEUE_NAME, durable:true, false, false);

            for (int i = 0; i < 100; i++)
            {
                var message = $" message - {i}";
                byte[] byteMessage = Encoding.UTF8.GetBytes(message);

                IBasicProperties properties = channel.CreateBasicProperties();
                properties.Persistent = true;//RabbitMQ'da Fiziksel kayda alınır.

                channel.BasicPublish(exchange: "", routingKey: QUEUE_NAME, body: byteMessage, basicProperties: properties);
                System.Console.WriteLine("Message published Success!");
            }

           
        }

        private static void SmartQueuePublisherWithFanoutExhange()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.ExchangeDeclare(FANOUT_EXCHANGE_NAME, ExchangeType.Fanout, true, false, null);

            for (int i = 0; i < 100; i++)
            {
                var message = $" message - {i}";
                byte[] byteMessage = Encoding.UTF8.GetBytes(message);

                IBasicProperties properties = channel.CreateBasicProperties();
                properties.Persistent = true;//RabbitMQ'da Fiziksel kayda alınır.

                channel.BasicPublish(exchange: FANOUT_EXCHANGE_NAME, routingKey: "", body: byteMessage, basicProperties: properties);
                System.Console.WriteLine("Message published Success!");
            }


        }

        private static void SmartQueuePublisherWithDirectExhange()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.ExchangeDeclare(DIRECT_EXCHANGE_NAME, ExchangeType.Direct, true, false, null);

            for (int i = 0; i < 100; i++)
            {
                var message = $" message - {i}";
                byte[] byteMessage = Encoding.UTF8.GetBytes(message);

                IBasicProperties properties = channel.CreateBasicProperties();
                properties.Persistent = true;//RabbitMQ'da Fiziksel kayda alınır.

                var routeKey = i % 2 == 0 ? "EVEN_NUMBERS" : "ODD_NUMBERS";
                channel.BasicPublish(exchange: DIRECT_EXCHANGE_NAME, routingKey: routeKey, body: byteMessage, basicProperties: properties);
                System.Console.WriteLine("Message published Success!");
            }
        }

        private static void SmartQueuePublisherWithTopicExhange()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.ExchangeDeclare(TOPIC_EXCHANGE_NAME, ExchangeType.Topic, true, false, null);

            for (int i = 0; i < 100; i++)
            {
                var message = $" message - {i}";
                byte[] byteMessage = Encoding.UTF8.GetBytes(message);

                IBasicProperties properties = channel.CreateBasicProperties();
                properties.Persistent = true;//RabbitMQ'da Fiziksel kayda alınır.

                var routeKey = $"SAYI.RAKAM.{(i % 2 == 0 ? "EVEN_NUMBERS" : i % 3 == 0 ? "MULTIPLES_OF_TREE":"ODD_NUMBERS")}";
                channel.BasicPublish(exchange: TOPIC_EXCHANGE_NAME, routingKey: routeKey, body: byteMessage, basicProperties: properties);
                System.Console.WriteLine("Message published Success!");
            }
        }

        private static void SmartQueuePublisherWithHeaderExhange()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.ExchangeDeclare(HEADER_EXCHANGE_NAME, ExchangeType.Headers, true, false, null);

            for (int i = 0; i < 100; i++)
            {
                var message = $" message - {i}";
                byte[] byteMessage = Encoding.UTF8.GetBytes(message);

                IBasicProperties properties = channel.CreateBasicProperties();
                properties.Persistent = true;//RabbitMQ'da Fiziksel kayda alınır.
                properties.Headers = new Dictionary<string, object>()
                {
                    {"Type", (i % 2 == 0 ? "EVEN_NUMBERS" : i % 3 == 0 ? "MULTIPLES_OF_TREE" : "ODD_NUMBERS")}
                };

                channel.BasicPublish(exchange: HEADER_EXCHANGE_NAME, routingKey: string.Empty, body: byteMessage, basicProperties: properties);
                System.Console.WriteLine("Message published Success!");
            }
        }

    }

}
