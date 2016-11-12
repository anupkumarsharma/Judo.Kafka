using System.Threading.Tasks;

namespace Judo.Kafka
{
    public interface IKafkaSerialzier
    {
        Task<byte[]> SerializeAsync<TPayload>(TPayload payload, bool isKey, string topic);

        Task<TPayload> DeserializeAsync<TPayload>(byte[] payload, bool isKey, string topic);
    }
}