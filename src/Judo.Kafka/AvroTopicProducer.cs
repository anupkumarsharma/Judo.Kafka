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

        public async Task<Message> ProduceAsync<TMessage>(TMessage payload, int partition = -1)
        {
            var serializeTask =  await _kafkaSerializer.SerializeAsync(payload, false, _topicName).ConfigureAwait(false);
            return await _producer.ProduceAsync(_topicName, null, serializeTask).ConfigureAwait(false);
        }

        public async Task<Message> ProduceAsync<TKey, TMessage>(TKey key, TMessage message, int partition = -1)
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