using System.Threading.Tasks;
using Confluent.Kafka;

namespace Judo.Kafka
{
    class AvroTopicProducer : ITopicProducer
    {
        private readonly string _topicName;
        private readonly Producer _producer;
        private readonly IKafkaSerialzier _kafkaSerializer;

        public AvroTopicProducer(Producer producer, string topicName, IKafkaSerialzier kafkaSerializer)
        {
            _producer = producer;
            _topicName = topicName;
            _kafkaSerializer = kafkaSerializer;
        }

        /// <summary>
        /// Produces a message to Kafka without a key
        /// </summary>
        /// <typeparam name="TMessage">The type of the object to produce</typeparam>
        /// <param name="payload">The object to produce</param>
        /// <returns></returns>
        public async Task<Message> ProduceAsync<TMessage>(TMessage payload)
        {
            var serializeTask = await _kafkaSerializer.SerializeAsync(payload, false, _topicName).ConfigureAwait(false);
            return await _producer.ProduceAsync(_topicName, null, serializeTask).ConfigureAwait(false);
        }

        /// <summary>
        /// Produces a message to Kafka with a key
        /// </summary>
        /// <typeparam name="TKey">The type of the key to produce</typeparam>
        /// <typeparam name="TMessage">The type of the object to produce</typeparam>
        /// <param name="key">The key to produce</param>
        /// <param name="message">The object to produce</param>
        /// <returns></returns>
        public async Task<Message> ProduceAsync<TKey, TMessage>(TKey key, TMessage message)
        {
            var keyPayloadTask = await _kafkaSerializer.SerializeAsync(key, true, _topicName).ConfigureAwait(false);
            var valuePayloadTask = await _kafkaSerializer.SerializeAsync(message, false, _topicName).ConfigureAwait(false);
            return await _producer.ProduceAsync(_topicName, keyPayloadTask, valuePayloadTask).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _producer.Dispose();
        }
    }
}