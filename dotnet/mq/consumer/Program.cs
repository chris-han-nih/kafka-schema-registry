using System.Diagnostics;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using converter;
using model;

var consumerConfig = new ConsumerConfig
                     {
                         BootstrapServers = "devops-kafka-b1-dev.greatgameplatform.com:9092,devops-kafka-b1-dev.greatgameplatform.com:9092",
                         GroupId = "test-consumer-group",
                         AutoOffsetReset = AutoOffsetReset.Earliest
                     };
var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://10.140.4.138:8081" };
var cts = new CancellationTokenSource();

using var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig);
using var consumer = new ConsumerBuilder<string, GenericRecord>(consumerConfig)
                    .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build();
// consumer.Subscribe("np-schema-registry-test");
consumer.Subscribe(new List<string> { "np-schema-registry-test" });

while(true)
{
    var sw = new Stopwatch();
    sw.Start();
    try
    {
        var consumeResult = consumer.Consume(cts.Token);
        Console.WriteLine($"Consumed record with value {consumeResult.Message.Value}");

        switch (consumeResult.Message.Value.Schema.Name.ToLower())
        {
            case "agent":
            {
                var agent = AvroRecord.MapToClass<Agent>(consumeResult.Message.Value);
                Console.WriteLine($"name: {agent.name}, logo: {agent.logo}, group number: {agent.group_number}");
                break;
            }
            case "user":
            {
                var user = AvroRecord.MapToClass<User>(consumeResult.Message.Value);
                Console.WriteLine($"name: {user.name}, favorite number: {user.favorite_number}");
                break;
            }
        }
    }
    catch (ConsumeException e)
    {
        Console.WriteLine($"Consume error: {e.Error.Reason}");
    }
    sw.Stop();
    Console.WriteLine($"consumed 1 message in {sw.ElapsedMilliseconds} ms");
}
