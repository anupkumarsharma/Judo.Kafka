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

        /// <summary>
        /// Creates a new Kafka producer connection
        /// </summary>
        /// <param name="schemaRegistryUrl">The schema registry URL.</param>
        /// <param name="configuration">Kafka configuration parameters (refer to https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
        ///  Topic configuration parameters are specified via the "default.topic.config" sub-dictionary config parameter.</param>
        /// <returns>A new Kafka producer connection.</returns>
        public static KafkaClient Connect(string schemaRegistryUrl, Dictionary<string, object> configuration)
        {
            return new KafkaClient(schemaRegistryUrl, configuration);
        }

        private KafkaClient(string schemaRegistryUrl, Dictionary<string, object> configuration)
        {
            _schemaRegistryUrl = schemaRegistryUrl;
            _configuration = configuration;
        }

        /// <summary>
        /// Initializes and returns a new producer instance 
        /// </summary>
        /// <param name="topicName">The topic that this producer will produce to.</param>
        /// <param name="useAvroDataContractResolver">A flag to use the AvroDataContractResolver; false will use the AvroPublicMemberResolver.</param>
        /// <returns>A new Kafka producer.</returns>
        public ITopicProducer GetTopicProducer(string topicName, bool useAvroDataContractResolver = false)
        {
            var producer = new Producer(_configuration, false, false);
            var schemaRegistryAvroSerializer = new SchemaRegistryAvroSerializer(new CachedSchemaRegistryClient(_schemaRegistryUrl, 200), useAvroDataContractResolver);
            return new AvroTopicProducer(producer, topicName, schemaRegistryAvroSerializer);
        }
    }
}