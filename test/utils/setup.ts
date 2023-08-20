/*
    Setup -- start dynamodb instance
 */
const waitPort = require('wait-port')
import DynamoDbLocal from 'dynamo-db-local'

const PORT = parseInt(process.env.PORT || '4567')

module.exports = async () => {
    let dynamodb = DynamoDbLocal.spawn({port: PORT})
    console.info('\nSpawn DynamoDB', dynamodb.pid)
    await waitPort({host: '0.0.0.0', port: PORT, timeout: 10000})
    process.env.DYNAMODB_PID = String(dynamodb.pid)
    process.env.DYNAMODB_PORT = String(PORT)

    // When jest throws anything unhandled, ensure we kill the spawned process
    process.on('unhandledRejection', (error) => {
        let pid = parseInt(process.env.DYNAMODB_PID || '')
        if (pid) {
            process.kill(pid)
        }
    })
}
