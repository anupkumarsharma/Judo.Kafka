using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Judo.SchemaRegistryClient;
using Microsoft.Hadoop.Avro;

namespace Judo.Kafka
{
    class SchemaRegistryAvroSerializer : IKafkaSerialzier
    {
        private readonly ISchemaRegistryClient _schemaRegistryClient;
        private readonly IDictionary<Type, object> _serializerCache = new Dictionary<Type, object>();
        private readonly bool _useAvroDataContractResolver;


        public SchemaRegistryAvroSerializer(ISchemaRegistryClient schemaRegistryClient, bool useAvroDataContractResolver)
        {
            _schemaRegistryClient = schemaRegistryClient;
            _useAvroDataContractResolver = useAvroDataContractResolver;
        }

        public Task<TPayload> DeserializeAsync<TPayload>(byte[] payload, bool isKey, string topic)
        {
            var subject = GetSubjectName(topic, isKey);
            var serializer = GetSerializer<TPayload>();
            using (var stream = new MemoryStream(payload))
            {
                stream.Seek(sizeof(byte) + sizeof(uint), SeekOrigin.Begin);
                return Task.FromResult(serializer.Deserialize(stream));
            }
        }

        public async Task<byte[]> SerializeAsync<TPayload>(TPayload payload, bool isKey, string topic)
        {
            var subject = GetSubjectName(topic, isKey);
            var serializer = GetSerializer<TPayload>();

            var schemaId = await _schemaRegistryClient.RegisterAsync(subject, serializer.ReaderSchema);
            var uintSchemaId = Convert.ToUInt32(schemaId);
            if (BitConverter.IsLittleEndian)
            {
                uintSchemaId = SwapEndianness(uintSchemaId);
            }

            using (var stream = new MemoryStream())
            {
                var sw = new BinaryWriter(stream);
                sw.Write((byte)0x0);
                sw.Write(uintSchemaId);
                sw.Flush();
                serializer.Serialize(stream, payload);
                stream.Seek(0, SeekOrigin.Begin);
                return stream.ToArray();
            }
        }

        private IAvroSerializer<TPayload> GetSerializer<TPayload>()
        {
            if (_serializerCache.ContainsKey(typeof(TPayload)))
            {
                return (IAvroSerializer<TPayload>)_serializerCache[typeof(TPayload)];
            }

            var serializer = AvroSerializer.Create<TPayload>(new AvroSerializerSettings()
            {
                Resolver =
                    _useAvroDataContractResolver
                        ? (AvroContractResolver) new AvroDataContractResolver(true)
                        : new AvroPublicMemberContractResolver(true),
                Surrogate = new AvroSurrogateStrategy()
            });

            _serializerCache.Add(typeof(TPayload), serializer);

            return serializer;
        }

        private string GetSubjectName(string topic, bool isKey)
        {
            return topic + (isKey ? "-key" : "-value");
        }

        uint SwapEndianness(uint x)
        {
            return ((x & 0x000000ff) << 24) +  // First byte
                   ((x & 0x0000ff00) << 8) +   // Second byte
                   ((x & 0x00ff0000) >> 8) +   // Third byte
                   ((x & 0xff000000) >> 24);   // Fourth byte
        }
    }
}