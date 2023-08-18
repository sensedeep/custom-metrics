import {DynamoDBClient} from '@aws-sdk/client-dynamodb'
import {OneSchema, Table} from 'dynamodb-onetable'
import {CustomMetrics, DefaultSpans, Metric, MetricQueryPoint, MetricQueryResult} from '../../src/index'
import {Schema} from '../../src/schema'
import SenseLogs from 'senselogs'

const PORT = parseInt(process.env.DYNAMODB_PORT || '4567')
const log = new SenseLogs({destination: 'stdout', format: 'human'})

const Client = new DynamoDBClient({
    endpoint: `http://localhost:${PORT}`,
    region: 'local',
    credentials: {
        accessKeyId: 'test',
        secretAccessKey: 'test',
    },
})

const dump = (...args) => {
    let s: string[] = []
    for (let item of args) {
        let values = JSON.stringify(
            item,
            function (key, value) {
                if (this[key] instanceof Date) {
                    return this[key].toLocaleString()
                }
                return value
            },
            4
        )
        s.push(values)
    }
    let result = s.join(' ')
    console.log(result)
    return result
}

const dumpMetrics = function (results: MetricQueryResult[]) {
    let buf: string[] = []
    for (let metric of results) {
        buf.push(`${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions)} ${metric.period}`)
        let points = metric.points.slice(0)
        for (let point of points) {
            buf.push(`     ${new Date(point.timestamp!).toLocaleString()} = ${point.value || '-'} / ${point.count}`)
        }
        print(buf.join('\n'))
    }
}

const print = (...args) => {
    console.log(...args)
}
globalThis.dump = dump
globalThis.print = print

const delay = async (time: number) => {
    return new Promise(function (resolve, reject) {
        setTimeout(() => resolve(true), time)
    })
}

export {Schema, Client, CustomMetrics, DefaultSpans, OneSchema, Table, delay, dump, dumpMetrics, log, print}
