namespace producer.Provider;

using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

public sealed class KafkaProducer
{
   private readonly IProducer<string, GenericRecord> _producer;

   private readonly ProducerConfig _producerConfig =
       new() { BootstrapServers = "localhost:29092", EnableIdempotence = true, Acks = Acks.All };
   private readonly SchemaRegistryConfig _schemaRegistryConfig = new() { Url = "http://localhost:8081" };
   private readonly AvroSerializerConfig _avroSerializerConfig = new() { BufferBytes = 1024, AutoRegisterSchemas = true };

   private KafkaProducer()
   {
      var schemaRegistry = new CachedSchemaRegistryClient(_schemaRegistryConfig);
      var nihSchemaRegistry = new NihSchemaRegistryClient(schemaRegistry);

      _producer = new ProducerBuilder<string, GenericRecord>(_producerConfig)
                 .SetKeySerializer(Serializers.Utf8)
                 .SetValueSerializer(new AvroSerializer<GenericRecord>(nihSchemaRegistry, _avroSerializerConfig))
                 .Build();
   }

   private static readonly Lazy<KafkaProducer> _instance = new(() => new KafkaProducer());
   public static KafkaProducer Instance => KafkaProducer._instance.Value;
   public IProducer<string, GenericRecord> Producer => _producer;
}