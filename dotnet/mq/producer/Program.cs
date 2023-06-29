using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using converter;
using model;
using producer;

var producerConfig = new ProducerConfig { BootstrapServers = "localhost:29092" };
var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://localhost:8081" };
var avroSerializerConfig = new AvroSerializerConfig { BufferBytes = 1024, AutoRegisterSchemas = true };

var cts = new CancellationTokenSource();

#region schemas
using var userStreamReader = new StreamReader("Avro/User.avsc");
var userSchemaStream = userStreamReader.ReadToEnd();
var userSchema = Avro.Schema.Parse(userSchemaStream);

using var agentStreamReader = new StreamReader("Avro/Agent.avsc");
var agentSchemaStream = agentStreamReader.ReadToEnd();
var agentSchema = Avro.Schema.Parse(agentSchemaStream);
#endregion

#region schema registry client
var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
var nihSchemaRegistry = new NihSchemaRegistryClient(schemaRegistry, 100);
#endregion

using var producer = new ProducerBuilder<string, GenericRecord>(producerConfig)
                    .SetKeySerializer(Serializers.Utf8)
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(nihSchemaRegistry, avroSerializerConfig))
                    .Build();

while (true)
{
    await Task.Delay(100, cts.Token);

     var agent = new Agent
                {
                    name = Guid.NewGuid().ToString(),
                    logo = Guid.NewGuid().ToString(),
                    group_number = new Random().Next(0, 100),
                };
     var user = new User
                {
                    name = Guid.NewGuid().ToString(),
                    favorite_number = new Random().Next(0, 100),
                };
    await produce(agent, agentSchema, cts);
    await produce(user, userSchema, cts);
}

async Task produce<T>(T body, Avro.Schema schema, CancellationTokenSource cancellationTokenSource) where T : class
{
    var record = AvroRecord.ToGenericRecord(body, (RecordSchema)schema);
    var message = new Message<string, GenericRecord> { Value = record };
    await producer.ProduceAsync("test", message)
                  .ContinueWith(task =>
                                {
                                    Console.WriteLine(!task.IsFaulted
                                                          ? $"produced to: {task.Result.TopicPartitionOffset}"
                                                          : $"error producing message: {task.Exception?.Message}");
                                },
                                cancellationTokenSource.Token);
}
