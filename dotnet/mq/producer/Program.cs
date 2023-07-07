using Avro;
using Avro.Generic;
using Confluent.Kafka;
using converter;
using model;
using producer.Provider;

var cts = new CancellationTokenSource();

#region schemas
using var userStreamReader = new StreamReader("Avro/User.avsc");
var userSchemaStream = userStreamReader.ReadToEnd();
var userSchema = Schema.Parse(userSchemaStream);

using var agentStreamReader = new StreamReader("Avro/Agent.avsc");
var agentSchemaStream = agentStreamReader.ReadToEnd();
var agentSchema = Schema.Parse(agentSchemaStream);
#endregion

while (true)
{
    await Task.Delay(1000, cts.Token);

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

async Task produce<T>(T body, Schema schema, CancellationTokenSource cancellationTokenSource) where T : class
{
    var record = AvroRecord.ToGenericRecord(body, (RecordSchema)schema);
    var message = new Message<string, GenericRecord> { Value = record };
    await KafkaProducer.Instance.Producer.ProduceAsync("test", message)
                  .ContinueWith(task =>
                                {
                                    Console.WriteLine(!task.IsFaulted
                                                          ? $"produced to: {task.Result.TopicPartitionOffset}, {task.Result.Value}"
                                                          : $"error producing message: {task.Exception?.Message}");
                                },
                                cancellationTokenSource.Token);
}
