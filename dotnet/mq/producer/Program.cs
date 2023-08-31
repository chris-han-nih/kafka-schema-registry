using System.Diagnostics;
using Avro;
using Avro.Generic;
using Confluent.Kafka;
using Confluent.SchemaRegistry;
using converter;
using model;
using producer.Provider;
using Schema = Avro.Schema;

var cts = new CancellationTokenSource();

#region schemas
using var userStreamReader = new StreamReader("Avro/User.avsc");
var userSchemaStream = userStreamReader.ReadToEnd();
var userSchema = Schema.Parse(userSchemaStream);

using var agentStreamReader = new StreamReader("Avro/Agent.avsc");
var agentSchemaStream = agentStreamReader.ReadToEnd();
var agentSchema = Schema.Parse(agentSchemaStream);
#endregion

#region Configurations
var producerConfig = new ProducerConfig
{
    BootstrapServers = "devops-kafka-b1-dev.greatgameplatform.com:9092,devops-kafka-b2-dev.greatgameplatform.com:9092",
    // EnableIdempotence = true,
    BatchSize = 1000,
    BatchNumMessages = 5000,
    Acks = Acks.Leader
    // MessageSendMaxRetries = 10, RetryBackoffMs = 1000, LingerMs = 1000, BatchNumMessages = 1000
};
var schemaRegistryConfig = new SchemaRegistryConfig { Url = "http://10.140.4.138:8081" };
var count = 0;
#endregion

// var producer = KafkaProducer.Instance(producerConfig, schemaRegistryConfig).Producer;

// var i = 0;
// while (true)
// {
//     count++;
//     var sw = new Stopwatch();
//     sw.Start();
    // await Task.Delay(100, cts.Token);

     // var agent = new Agent
     //            {
     //                name = Guid.NewGuid().ToString(),
     //                logo = Guid.NewGuid().ToString(),
     //                group_number = new Random().Next(0, 100),
     //            };
     // var user = new User
     //            {
     //                name = Guid.NewGuid().ToString(),
     //                favorite_number = new Random().Next(0, 100),
     //            };
    // await produce(agent, agentSchema, cts);
    // await produce(user, userSchema, cts);
    // var u = new nsus.platform.user
    //         {
    //             name = Guid.NewGuid().ToString(),
    //             favorite_number = i++,
    //         };
    // await produce(u, u.Schema, cts);
// await produce(p, p.Schema, cts);
var p = new nsus.platform.person
{
    id = Guid.NewGuid().ToString(),
    name = Guid.NewGuid().ToString(),
    address = new nsus.platform.address1
    {
        street = Guid.NewGuid().ToString(),
        city = Guid.NewGuid().ToString(),
        state = Guid.NewGuid().ToString(),
        zip = Guid.NewGuid().ToString()
    }
};
// var a = AvroRecord.ToGenericRecord(p, p.Schema);
var ps = new nsus.platform.persons { };
await produce(ps, cts);
return;
//     sw.Stop();
//     Console.WriteLine($"produced {count} messages in {sw.ElapsedMilliseconds} ms");
// }

async Task produce(nsus.platform.persons persons, CancellationTokenSource cancellationTokenSource) 
{
    try
    {
        var producer = KafkaProducer.Instance(producerConfig, schemaRegistryConfig)
                                    .Producer;
        var record = new GenericRecord((RecordSchema)persons.Schema);
        record.Add("id", Guid.NewGuid().ToString());
        record.Add("name", Guid.NewGuid().ToString());
        record.Add("age", new Random().Next(0, 100));
        record.Add("address", Guid.NewGuid().ToString());
        record.Add("numbers", new List<string> { Guid.NewGuid().ToString(), Guid.NewGuid().ToString() });
        // var record = new GenericRecord((RecordSchema)person.Schema);
        // record.Add("id", Guid.NewGuid().ToString());
        // record.Add("name", Guid.NewGuid().ToString());
        //
        // var recordAddress = new GenericRecord((RecordSchema)person.address.Schema);
        // recordAddress.Add("street", Guid.NewGuid().ToString());
        // recordAddress.Add("city", Guid.NewGuid().ToString());
        // recordAddress.Add("state", Guid.NewGuid().ToString());
        // recordAddress.Add("zip", Guid.NewGuid().ToString());
        // record.Add("address", recordAddress);
        // var record = AvroRecord.ToGenericRecord(body, (RecordSchema)schema);
        var message = new Message<string, GenericRecord> { Value = record };
        await producer.ProduceAsync("np-schema-registry-test", message);
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.Message);
    }
    // .ContinueWith(task =>
    //               {
    //                   Console.WriteLine(!task.IsFaulted
    //                   ? $"produced to: {task.Result.TopicPartitionOffset}"
    //                   : $"error producing message: {task.Exception?.Message}");
    //               },
    //               cancellationTokenSource.Token);
}
