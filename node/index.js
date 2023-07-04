const path = require('path')
const { Kafka } = require('kafkajs')
const { SchemaRegistry, SchemaType, avdlToAVSCAsync } = require('@kafkajs/confluent-schema-registry')

const registry = new SchemaRegistry({ host: 'http://localhost:8081' })
const kafka = new Kafka({
  brokers: ['localhost:29092'],
  clientId: 'node-consumer',
})
const consumer = kafka.consumer({ groupId: 'node-test-group',  })

const Topic = 'test'

const run = async () => {

  await consumer.connect()
  await consumer.subscribe({ topic: Topic })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decodedMessage = {
        ...message,
        value: await registry.decode(message.value)
      }
      console.log(decodedMessage);
    },
  })
}

run().catch(async e => {
  console.error(e)
  consumer && await consumer.disconnect()
  process.exit(1)
})
