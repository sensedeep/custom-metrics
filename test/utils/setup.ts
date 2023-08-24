/*
    Setup -- setup for the test run
 */
import {DynamoDBClient} from '@aws-sdk/client-dynamodb'
import {CreateTableCommand, DescribeTableCommand} from '@aws-sdk/client-dynamodb'
import DynamoDbLocal from 'dynamo-db-local'
import waitPort from 'wait-port'

const PORT = parseInt(process.env.PORT || '4765')

module.exports = async () => {
    /*
        Start the local dynamodb
     */
    let dynamodb = DynamoDbLocal.spawn({port: PORT})
    console.info('\nSpawn DynamoDB', dynamodb.pid)
    await waitPort({host: '0.0.0.0', port: PORT, timeout: 10000})
    process.env.DYNAMODB_PID = String(dynamodb.pid)
    process.env.DYNAMODB_PORT = String(PORT)

    /*
        Create the AWS client
     */
    const client = new DynamoDBClient({
        endpoint: `http://localhost:${PORT}`,
        region: 'local',
        credentials: {accessKeyId: 'test', secretAccessKey: 'test'},
    })

    await createTable(client, 'CustomMetrics')

    globalThis.DynamoDBClient = client

    // When jest throws anything unhandled, ensure we kill the spawned process
    process.on('unhandledRejection', (error) => {
        let pid = parseInt(process.env.DYNAMODB_PID || '')
        if (pid) {
            process.kill(pid)
        }
    })
}

async function createTable(client, table) {
    let def = {
        AttributeDefinitions: [
            {AttributeName: 'pk', AttributeType: 'S'},
            {AttributeName: 'sk', AttributeType: 'S'},
        ],
        KeySchema: [
            {AttributeName: 'pk', KeyType: 'HASH'},
            {AttributeName: 'sk', KeyType: 'RANGE'},
        ],
        TableName: table,
        BillingMode: 'PAY_PER_REQUEST',
    }
    let command = new CreateTableCommand(def)
    await client.send(command)

    /*
        Wait for the table to become live
     */
    let deadline = Date.now() + 10 * 1000
    do {
        let command = new DescribeTableCommand({TableName: 'CustomMetrics'})
        let info = await client.send(command)
        if (info.Table.TableStatus == 'ACTIVE') {
            break
        }
        if (deadline < Date.now()) {
            throw new Error('Table has not become active')
        }
        await delay(1000)
    } while (Date.now() < deadline)
    globalThis.TableName = table
}

const delay = async (time: number) => {
    return new Promise(function (resolve, reject) {
        setTimeout(() => resolve(true), time)
    })
}
