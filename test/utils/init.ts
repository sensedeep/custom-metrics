import {CustomMetrics, DefaultSpans, Metric, MetricQueryResult, SpanDef} from '../../src/index'
import SenseLogs from 'senselogs'

const log = new SenseLogs({destination: 'stdout', format: 'human'})

/*
    Share the client and table created in setup.ts
 */
const client = globalThis.DynamoDBClient
const table = globalThis.TableName

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

const dumpMetric = function (metric: Metric) {
    let buf: string[] = []
    buf.push(`${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions)}`)
    for (let span of metric.spans) {
        buf.push(` ${span.period}, ${new Date(span.end * 1000).toLocaleString()} ${span.points.length} points`)
        for (let point of span.points) {
            buf.push(`     count ${point.count} = sum ${point.sum}`)
        }
    }
    print(buf.join('\n'))
}

const dumpQuery = function (metric: MetricQueryResult) {
    let points = metric.points.slice(0)
    let buf: string[] = []
    buf.push(`${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions)} ${metric.period} ${points.length} points`)
    for (let point of points) {
        buf.push(`     ${new Date(point.timestamp!).toLocaleString()} = ${point.value || '-'} / ${point.count}`)
    }
    print(buf.join('\n'))
}

const print = (...args) => {
    console.log(...args)
}
globalThis.dump = dump
globalThis.dumpMetric = dumpMetric
globalThis.dumpQuery = dumpQuery
globalThis.print = print

const delay = async (time: number) => {
    return new Promise(function (resolve, reject) {
        setTimeout(() => resolve(true), time)
    })
}

export {table, client, CustomMetrics, DefaultSpans, SpanDef, delay, dump, dumpMetric, dumpQuery, log, print}
