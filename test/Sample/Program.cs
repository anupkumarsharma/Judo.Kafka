using Confluent.Kafka;
using Judo.Kafka;
using Judo.SchemaRegistryClient;
using System;
using System.Collections.Generic;

namespace Sample
{
    public class TestClass
    {
        public string Name { get; set; }
        public int Number { get; set; }
        public DateTime Timestamp { get; set; }

        public override string ToString()
            => $"Name={Name}, Number${Number}, Timestamp={Timestamp:o}";
    }

    public class AvroSample
    {
        static void Main(string[] args)
        {
            string bootstrapservers = "172.22.12.3:49092";
            string schemaRegistryUri = "http://172.22.12.3:48081";
            string topic = "testAvro";

            int nbMessages = 100;
            var shemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUri, 200);

            //on 0.9.5, you cant produce on multiple topic with typed producer with the same avro serializer
            var judaSerializer = new SchemaRegistryAvroSerializer(shemaRegistryClient, false);
            var valueSerializer = new AvroSerializer<TestClass>(judaSerializer, topic, false);
            var keySerializer = new AvroSerializer<string>(judaSerializer, topic, true);

            var config = new Dictionary<string, object>
            {
                ["bootstrap.servers"] = bootstrapservers
            };
            using (var producer = new Producer<string, TestClass>(config, keySerializer, valueSerializer))
            {
                for (int i = 0; i < nbMessages; i++)
                {
                    var obj = new TestClass
                    {
                        Name = "name " + i,
                        Number = i,
                        Timestamp = DateTime.Now
                    };
                    producer.ProduceAsync(topic, obj.Name, obj).ContinueWith(t=>
                    {
                        if(t.IsFaulted)
                        {
                            Console.WriteLine(t.Exception.Message);
                        }
                        else if(t.Result.Error.HasError)
                        {
                            Console.WriteLine(t.Result.Error);
                        }
                    });
                }
                Console.WriteLine("remaining: " + producer.Flush(10000));
            }
            
            //consume

            config = new Dictionary<string, object>
            {
                ["bootstrap.servers"] = bootstrapservers,
                ["group.id"] = Guid.NewGuid(),
                ["auto.offset.reset"] = "smallest"
            };
            var valueDeserializer = new AvroDeserializer<TestClass>(judaSerializer, "testTopic", false);
            var keyDeserializer = new AvroDeserializer<string>(judaSerializer, "testTopic", true);
            using (var consumer = new Consumer<string, TestClass>(config, keyDeserializer, valueDeserializer))
            {
                //on 0.9.5, you cant produce on multiple topic with the same serializer
                int count = 0;
                consumer.OnMessage += (o, e) =>
                {
                    count++;
                    Console.WriteLine($"{e.Key} : {e.Value}");
                };

                consumer.OnError += (o, e)
                    => Console.WriteLine(e);

                consumer.OnConsumeError += (o, e) =>
                {
                    count++;
                    Console.WriteLine(e.Error);
                };
                
                consumer.Subscribe(topic);

                while (count != nbMessages)
                {
                    consumer.Poll(100);
                }
            }
        }
    }
}