using System;
using Judo.SchemaRegistryClient;
using RdKafka;

namespace Judo.Kafka
{
    public class KafkaClient 
    {
        private readonly string[] _bootstrapServers;
        private readonly string _schemaRegistryUrl;
        private Config _config;
        public static KafkaClient Connect(string[] bootstrapServers, string schemaRegistryUrl, Config config = null)
        {
            return new KafkaClient(bootstrapServers, schemaRegistryUrl, config);
        }

        private KafkaClient(string[] bootstrapServers, string schemaRegistryUrl, Config config)
        {
            _bootstrapServers = bootstrapServers;
            _schemaRegistryUrl = schemaRegistryUrl;
            _config = config;
        }

        public ITopicProducer GetTopicProducer(string topicName, TopicConfig cfg = null)
        {
            var producer = new Producer(_config, string.Join(",", _bootstrapServers));
            var topic = producer.Topic(topicName, cfg);
            var schemaRegistryAvroSerializer = new SchemaRegistryAvroSerializer(new CachedSchemaRegistryClient(_schemaRegistryUrl, 200));
            return new AvroTopicProducer(producer, topic, schemaRegistryAvroSerializer);
        }
    }
}