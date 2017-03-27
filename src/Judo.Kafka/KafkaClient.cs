using System;
using System.Collections.Generic;
using Judo.SchemaRegistryClient;
using Confluent.Kafka;

namespace Judo.Kafka
{
    public class KafkaClient
    {
        private readonly string _schemaRegistryUrl;
        private readonly Dictionary<string, object> _configuration;

        public static KafkaClient Connect(string schemaRegistryUrl, Dictionary<string, object> configuration)
        {
            return new KafkaClient(schemaRegistryUrl, configuration);
        }

        private KafkaClient(string schemaRegistryUrl, Dictionary<string, object> configuration)
        {
            _schemaRegistryUrl = schemaRegistryUrl;
            _configuration = configuration;
        }

        public ITopicProducer GetTopicProducer(string topicName, bool useAvroDataContractResolver = false)
        {
            var producer = new Producer(_configuration, false, false);
            var schemaRegistryAvroSerializer = new SchemaRegistryAvroSerializer(new CachedSchemaRegistryClient(_schemaRegistryUrl, 200), useAvroDataContractResolver);
            return new AvroTopicProducer(producer, topicName, schemaRegistryAvroSerializer);
        }
    }
}