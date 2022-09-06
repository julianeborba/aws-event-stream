jest.deepUnmock('aws-sdk');
jest.unmock('aws-sdk/clients/dynamodb');
jest.setTimeout(1000000);

import * as AWS from "aws-sdk";
import { SQS } from "aws-sdk";
import { DocumentClient } from 'aws-sdk/clients/dynamodb';
import { Config, DynamodbProvider, EventStore } from "../../src";
import { AWSConfig } from "../../src/aws/config";
import { SNSPublisher } from "../../src/publisher/sns";
import DynamoDB = require('aws-sdk/clients/dynamodb');


describe.only('EventStream', () => {

    AWS.config.update(
        {
            credentials: {
                accessKeyId: 'mock_access_key',
                secretAccessKey: 'mock_access_key',
            },
            region: "us-east-1"
        });

    const awsConfig: AWSConfig = {
        credentials: {
            accessKeyId: 'mock_access_key',
            secretAccessKey: 'mock_access_key',
        },
        region: "us-east-1"
    };

    const dynamodbConfig: Config = {
        awsConfig: awsConfig,
        dynamodb: {
            endpointUrl: 'http://localhost:4566',
            tableName: 'order-events',
        },
    };

    const sqs = new SQS({
        endpoint: 'http://localhost:4566',
        region: 'us-east-1',
    });

    const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
    
    it('publish a message', async () => {

        // const params = {
        //     // Remove DelaySeconds parameter and value for FIFO queues
        //     DelaySeconds: 10,
        //     MessageAttributes: {
        //         "Author": {
        //             DataType: "String",
        //             StringValue: "John Grisham"
        //         },
        //         "Title": {
        //             DataType: "String",
        //             StringValue: "The Whistler"
        //         },
        //         "WeeksOn": {
        //             DataType: "Number",
        //             StringValue: "6"
        //         }
        //     },
        //     MessageBody: "Information about current NY Times fiction bestseller for week of 12/11/2016.",
        //     QueueUrl: "http://localhost:4566/000000000000/order-events-placed"
        // };
        // await sqs.sendMessage(params).promise();
        // await sleep(10000);

        // const message = await sqs.receiveMessage({
        //     AttributeNames: ['All'],
        //     MessageAttributeNames: ['All'],
        //     QueueUrl: 'http://localhost:4566/000000000000/order-events-placed',
        // }).promise();
        // expect(message).toEqual({});

        const eventStore = new EventStore(
            new DynamodbProvider(dynamodbConfig),
            new SNSPublisher('arn:aws:sns:us-east-1:000000000000:order-events', awsConfig, { endpointUrl: 'http://localhost:4566' }),
        );
        const event = {
            document: {
                identifier: '321'
            },
            eventType: 'PLACED',
            proposal: {
                id: '123',
                offer: 'new-offer'
            },
            version: 1,
        };

        const eventPlaced = await eventStore.getEventStream('Order', '123456').addEvent(event);

        await sleep(10000);

        const messageReceived = await sqs.receiveMessage({
            AttributeNames: ['All'],
            MessageAttributeNames: ['All'],
            QueueUrl: 'http://localhost:4566/000000000000/order-events-placed',
        }).promise();

        expect(messageReceived).not.toBeUndefined();
        expect(messageReceived.Messages).not.toBeUndefined();

        const eventReceived = JSON.parse(JSON.parse(messageReceived.Messages[0].Body).Message);

        expect(eventReceived.event.commitTimestamp).not.toBeUndefined();
        expect(eventReceived.event).toEqual(
            expect.objectContaining({
                eventType: "PLACED",
                payload: {
                    document: {
                        identifier: "321",
                    },
                    eventType: "PLACED",
                    proposal: {
                        id: "123",
                        offer: "new-offer",
                    },
                    version: 1,
                },
            })
        );
        expect(eventPlaced.commitTimestamp).not.toBeUndefined();

        expect(eventPlaced).toEqual(
            expect.objectContaining({
                eventType: "PLACED",
                payload: {
                    document: {
                        identifier: "321",
                    },
                    eventType: "PLACED",
                    proposal: {
                        id: "123",
                        offer: "new-offer",
                    },
                    version: 1,
                },
            })
        );

        const events = await eventStore.getEventStream('Order', '123456').getEvents();

        expect(events[0].payload).toEqual({
            document: {
                identifier: "321",
            },
            eventType: "PLACED",
            proposal: {
                id: "123",
                offer: "new-offer",
            },
            version: 1,
        });
        expect(events[0].sequence).toEqual(0);
    });

    it('should not put item in dynamo when eventType alredy exists', async () => {
        const documentClient: DocumentClient = new DynamoDB.DocumentClient({ endpoint: 'http://localhost:4566' });

        const record = {
            ConditionExpression: '(attribute_not_exists(aggregation_streamid) AND attribute_not_exists(eventType.PAYED))',
            Item: {
            aggregation_streamid: 'orders:11',
            commitTimestamp: 1611206813,
            eventType: 'SENT',
            payload: {
                eventType: 'SENT',
                text: 'EVENT PAYLOAD',
            },
            stream: {
                aggregation: 'orders',
                id: '1',
            },
            },
            TableName: 'order-events',
        };

        const payedRecord = {
            ConditionExpression: '(attribute_not_exists(aggregation_streamid) AND attribute_not_exists(eventType.PAYED))',
            Item: {
            aggregation_streamid: 'orders:11',
            commitTimestamp: 1611206823,
            eventType: 'PAYED',
            payload: {
                eventType: 'PAYED',
                text: 'EVENT PAYLOAD',
            },
            stream: {
                aggregation: 'orders',
                id: '1',
            },
            },
            TableName: 'order-events',
        };


        const putSentEventType = await documentClient.put(record).promise();
        const putPayedEventType = await documentClient.put(payedRecord).promise();

        expect(putSentEventType).not.toBe(null);
        expect(putPayedEventType).not.toBe(null);
        await expect(documentClient.put(payedRecord).promise()).rejects.toThrowError();
    });

  it.only('Xshould not put item in dynamo when eventType alredy exists', async () => {

    const dynamodbConfig: Config = {
        awsConfig: awsConfig,
        dynamodb: {
            endpointUrl: 'http://localhost:4566',
            tableName: 'order-events',
            conditionExpression: '(attribute_not_exists(eventType) AND attribute_not_exists(eventType.PAYED))'
        },
    };

    const eventStore = new EventStore(
        new DynamodbProvider(dynamodbConfig)
    );
   
    const record = {
        aggregation_streamid: 'orders:20',
        document: {
            identifier: '321'
        },
        eventType: 'SENT',
        proposal: {
            id: '123',
            offer: 'new-offer'
        },
        version: 1,
    };

    const payedRecord = {
        aggregation_streamid: 'orders:20',
        document: {
            identifier: '321'
        },
        eventType: 'PAYED',
        proposal: {
            id: '123',
            offer: 'new-offer'
        },
        version: 1,
    };



    await eventStore.getEventStream('orders', '20').addEvent(record);

    const events = await eventStore.getEventStream('orders', '20').getEvents();
    console.log(' verificacao 1', events);
    //expect(events.length).toEqual(1);


    await eventStore.getEventStream('orders', '20').addEvent(payedRecord);
    const events2 = await eventStore.getEventStream('orders', '20').getEvents();
    console.log('verificacao 2', events2);

    await expect(eventStore.getEventStream('orders', '20').addEvent(payedRecord)); //.rejects.toThrowError();
  });
});
