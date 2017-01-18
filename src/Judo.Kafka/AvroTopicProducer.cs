using System.Threading.Tasks;
using RdKafka;

namespace Judo.Kafka
{
    class AvroTopicProducer : ITopicProducer
    {
        private readonly Topic _topic;
        private readonly Producer _producer;
        private readonly IKafkaSerialzier _kafkaSerializer;

        public AvroTopicProducer(Producer producer, Topic topic, IKafkaSerialzier kafkaSerializer)
        {
            _producer = producer;
            _topic = topic;
            _kafkaSerializer = kafkaSerializer;
        }

        public async Task<DeliveryReport> ProduceAsync<TMessage>(TMessage payload, int partition = -1)
        {
            var serializeTask =  await _kafkaSerializer.SerializeAsync(payload, false, _topic.Name).ConfigureAwait(false);
            return await _topic.Produce(serializeTask, partition: partition).ConfigureAwait(false);
        }

        public async Task<DeliveryReport> ProduceAsync<TKey, TMessage>(TKey key, TMessage message, int partition = -1)
        {
            var keyPayloadTask = await _kafkaSerializer.SerializeAsync(key, true, _topic.Name).ConfigureAwait(false);
            var valuePayloadTask = await _kafkaSerializer.SerializeAsync(message, false, _topic.Name).ConfigureAwait(false);
            return await _topic.Produce(valuePayloadTask, keyPayloadTask, partition: partition).ConfigureAwait(false);
        }

        public void Dispose()
        {
            _topic.Dispose();
            _producer.Dispose();
        }
    }
}