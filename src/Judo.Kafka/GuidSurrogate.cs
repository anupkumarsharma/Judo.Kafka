namespace Judo.Kafka
{
    using System;
    using System.Linq;


    class GuidSurrogate : IAvroSurrogateStrategy
    {

        private static readonly Type[] GuidTypes = new[] { typeof(Guid), typeof(Guid?) };

        public object GetDeserializedObject(object obj, Type targetType)
        {
            if (IsGuid(obj.GetType()))
            {
                return Guid.Parse(obj.ToString());
            }

            return obj;
        }

        public object GetObjectToSerialize(object obj, Type targetType)
        {
            if (IsGuid(obj.GetType()))
            {
                return obj?.ToString();
            }

            return obj;
        }

        public Type GetSurrogateType(Type type)
        {
            if (IsGuid(type))
            {
                return typeof(string);
            }
            return type;
        }

        public bool SurrogateFor(Type type)
        {
            return IsGuid(type);
        }

        private bool IsGuid(Type type)
        {
            return GuidTypes.Contains(type);
        }
    }
}