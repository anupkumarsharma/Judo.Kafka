

namespace Judo.Kafka
{
    using System;
    using Xunit;
    using System.Threading.Tasks;
    using Judo.Kafka;
    using Judo.SchemaRegistryClient;
    using Microsoft.Hadoop.Avro.Schema;
    using NSubstitute;

    public class SerializerTests
    {
        [Fact]
        public async void SerializesToKafkaConnectCompatibleTypes() 
        {
            const string mockTopic = "topic";
            var mockReg = Substitute.For<ISchemaRegistryClient>();
            mockReg.RegisterAsync(mockTopic, Arg.Any<Schema>())
                .Returns(Task.FromResult(1));
            
            var serializer = new SchemaRegistryAvroSerializer(mockReg, false);
            var id = Guid.NewGuid();
            var someDate = DateTime.UtcNow;
            var offset = new DateTimeOffset(someDate);
            var epoch = (long)(someDate - new DateTime(1970,1,1)).TotalMilliseconds;
            var offsetEpoch = (long)(offset - new DateTime(1970,1,1)).TotalMilliseconds;
            var original = new TestObjectSource()
            {
                Id = id,
                SomeTime = someDate,
                SomeOffset = offset
            };
            var bytes = await serializer.SerializeAsync(original, false, mockTopic);

            var target = await serializer.DeserializeAsync<TestObjectTarget>(bytes, false, mockTopic);
            Assert.Equal(id.ToString(), target.Id);
            Assert.Equal(someDate.ToString(DateTimeSurrogate.IsoFormat), target.SomeTime);
            Assert.Equal(someDate.ToString(DateTimeSurrogate.IsoFormat), target.SomeOffset);

            var originalDeserialized = await serializer.DeserializeAsync<TestObjectSource>(bytes, false, mockTopic);
            Assert.True(originalDeserialized.Equals(original));
        }
    }

    public class TestObjectSource
    {
        public Guid Id {get;set;}

        public DateTime SomeTime {get;set;}

        public DateTimeOffset SomeOffset{get;set;}

        // override object.Equals
        public override bool Equals (object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }
            
            var other = obj as TestObjectSource;
            if(other.Id != this.Id)
            {
                return false;
            }
            var dateDiff = other.SomeTime - this.SomeTime;
            if(dateDiff.TotalMilliseconds > 1)
            {
                return false;
            }
            var dateOffsetDiff = other.SomeOffset - this.SomeOffset;
            if(dateOffsetDiff.TotalMilliseconds > 1)
            {
                return false;
            }
           
           return true;
        }
        
    }

    public class TestObjectTarget
    {
        public string Id {get;set;}

        public string SomeTime {get;set;}

        public string SomeOffset{get;set;}
    }
}
