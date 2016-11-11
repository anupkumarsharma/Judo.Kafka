using System.Threading.Tasks;

namespace Judo.Kafka
{
    interface IKafkaSerialzier
    {
        Task<byte[]> SerializeAsync<TPayload>(TPayload payload, bool isKey, string topic);
    }
}