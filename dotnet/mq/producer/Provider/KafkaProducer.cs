namespace producer.Provider;

using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

public sealed class KafkaProducer
{
    private static Lazy<KafkaProducer> instance;

    private readonly AvroSerializerConfig _avroSerializerConfig =
        new() { BufferBytes = 1024, AutoRegisterSchemas = true };

    private KafkaProducer(ProducerConfig producerConfig, SchemaRegistryConfig schemaRegistryConfig)
    {
        var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
        var nihSchemaRegistry = new NihSchemaRegistryClient(schemaRegistry);

        Producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
                  .SetKeySerializer(Serializers.Utf8)
                  .SetValueSerializer(new AvroSerializer<GenericRecord>(nihSchemaRegistry, _avroSerializerConfig))
                  .Build();
    }

    public static KafkaProducer Instance(ProducerConfig producerConfig, SchemaRegistryConfig schemaRegistryConfig)
    {
        if (KafkaProducer.instance != null) return KafkaProducer.instance.Value;

        KafkaProducer.instance =
            new Lazy<KafkaProducer>(() => new KafkaProducer(producerConfig, schemaRegistryConfig));
        return KafkaProducer.instance.Value;
    }

    public IProducer<string, GenericRecord> Producer { get; }
}