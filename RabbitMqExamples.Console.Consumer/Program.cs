using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqExamples.Console.Consumer
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
                BasicConsumer();
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

        private static void BasicConsumer()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
           
            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(QUEUE_NAME, false, consumer);
            consumer.Received += (sender, e) =>
            {
                System.Console.WriteLine(Encoding.UTF8.GetString(e.Body.ToArray())); 
            };
        }

        private static void SmartQueueConsumer()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();
            channel.BasicQos(0, 1, true);
            //‘BasicQos’ metodunun;
            //    prefetchSize parametresi : Mesaj boyutunu ifade eder. 0(sıfır) diyerek ilgilenmediğimizi belirtiyoruz.
            //    prefetchCount parametresi : Dağıtım adetini ifade eder.
            //    global parametresi : true ise, tüm consumerların aynı anda prefetchCount

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(QUEUE_NAME, false, consumer);
            consumer.Received += (sender, e) =>
            {
                System.Console.WriteLine(Encoding.UTF8.GetString(e.Body.ToArray()));
                channel.BasicAck(e.DeliveryTag, false);
            };
        }

        private static void SmartQueueConsumerWithFanoutExchange()
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();

            channel.ExchangeDeclare(FANOUT_EXCHANGE_NAME, ExchangeType.Fanout);

            var queueName = channel.QueueDeclare().QueueName;
            channel.QueueBind(queueName,FANOUT_EXCHANGE_NAME,routingKey: "");
            
            
            channel.BasicQos(0, 1, true);

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queueName, false, consumer);
            consumer.Received += (sender, e) =>
            {
                System.Console.WriteLine(Encoding.UTF8.GetString(e.Body.ToArray()));
                channel.BasicAck(e.DeliveryTag, false);
            };
        }

        private static void SmartQueueConsumerWithDirectExchange(bool isEvenNumber)
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();

            channel.ExchangeDeclare(DIRECT_EXCHANGE_NAME,ExchangeType.Direct);

            var queueName = channel.QueueDeclare().QueueName;
            var routeKey = isEvenNumber ? "EVEN_NUMBERS" : "ODD_NUMBERS";
            channel.QueueBind(queueName, DIRECT_EXCHANGE_NAME, routingKey: routeKey);


            channel.BasicQos(0, 1, true);

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queueName, false, consumer);
            consumer.Received += (sender, e) =>
            {
                System.Console.WriteLine(Encoding.UTF8.GetString(e.Body.ToArray()));
                channel.BasicAck(e.DeliveryTag, false);
            };
        }
        private static void SmartQueueConsumerWithTopicExchange(int numbers)
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();

            channel.ExchangeDeclare(TOPIC_EXCHANGE_NAME, ExchangeType.Topic);

            var queueName = channel.QueueDeclare().QueueName;
            var routeKey = $"SAYI.RAKAM.{(numbers % 2 == 0 ? "EVEN_NUMBERS" : numbers % 3 == 0 ? "MULTIPLES_OF_TREE" : "ODD_NUMBERS")}";
            channel.QueueBind(queueName, TOPIC_EXCHANGE_NAME, routingKey: routeKey);


            channel.BasicQos(0, 1, true);

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queueName, false, consumer);
            consumer.Received += (sender, e) =>
            {
                System.Console.WriteLine(Encoding.UTF8.GetString(e.Body.ToArray()));
                channel.BasicAck(e.DeliveryTag, false);
            };
        }

        private static void SmartQueueConsumerWithHeaderExchange(int numbers)
        {
            ConnectionFactory factory = new ConnectionFactory { HostName = RABBITMQ_HOST };

            using IConnection connection = factory.CreateConnection();
            using IModel channel = connection.CreateModel();

            channel.ExchangeDeclare(HEADER_EXCHANGE_NAME, ExchangeType.Headers);

            var queueName = channel.QueueDeclare().QueueName;
            var headerProperties = new Dictionary<string, object>()
            {
                {"x-match","all"/*any*/ },
                {"Type", (numbers % 2 == 0 ? "EVEN_NUMBERS" : numbers % 3 == 0 ? "MULTIPLES_OF_TREE" : "ODD_NUMBERS")}
            };
            channel.QueueBind(queueName, HEADER_EXCHANGE_NAME, routingKey: string.Empty, headerProperties);


            channel.BasicQos(0, 1, true);

            EventingBasicConsumer consumer = new EventingBasicConsumer(channel);
            channel.BasicConsume(queueName, false, consumer);
            consumer.Received += (sender, e) =>
            {
                System.Console.WriteLine(Encoding.UTF8.GetString(e.Body.ToArray()));
                channel.BasicAck(e.DeliveryTag, false);
            };
        }
    }
}
