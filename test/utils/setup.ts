/*
    Setup -- start dynamodb instance
 */
const waitPort = require('wait-port')
import DynamoDbLocal from 'dynamo-db-local'

const PORT = parseInt(process.env.PORT || '4567')

module.exports = async () => {
    console.info('Using local Java to run dynamoDB')
    let dynamodb = DynamoDbLocal.spawn({port: PORT, stdio: 'inherit'})

    console.info('Spawn DynamoDB', dynamodb.pid)
    await waitPort({host: '0.0.0.0', port: PORT, timeout: 10000})
    process.env.DYNAMODB_PID = String(dynamodb.pid)
    process.env.DYNAMODB_PORT = String(PORT)
    console.info('DynamoDB is ready')

    // When jest throws anything unhandled, ensure we kill the spawned process
    process.on('unhandledRejection', (error) => {
        let pid = parseInt(process.env.DYNAMODB_PID || '')
        if (pid) {
            process.kill(pid)
        }
    })
}
