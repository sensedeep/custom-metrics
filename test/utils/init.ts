import {CustomMetrics, DefaultSpans, Metric, MetricQueryResult, SpanDef} from '../../src/index'
import SenseLogs from 'senselogs'

const log = new SenseLogs({destination: 'stdout', format: 'human'})

/*
    Share the client and table created in setup.ts
 */
const client = globalThis.DynamoDBClient
const table = globalThis.TableName

//  Format date
function fmtdate(n) {
    function padTo2Digits(num) {
        return num.toString().padStart(2, '0')
    }
    let date = new Date(n)
    const year = date.getUTCFullYear().toString().slice(-2) // Get last two digits of the year
    const month = padTo2Digits(date.getUTCMonth() + 1) // Months are zero-indexed
    const day = padTo2Digits(date.getUTCDate())
    const hours = padTo2Digits(date.getUTCHours())
    const minutes = padTo2Digits(date.getUTCMinutes())
    const seconds = padTo2Digits(date.getUTCSeconds())
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
}

function dt(n: number) {
    return fmtdate(n * 1000)
}

const dump = (...args) => {
    let s: string[] = []
    for (let item of args) {
        let values = JSON.stringify(
            item,
            function (key, value) {
                if (this[key] instanceof Date) {
                    return fmtdate(this[key].getTime())
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
    buf.push(`${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions) || ''}`)
    for (let span of metric.spans) {
        let interval = span.period / span.samples
        let start = span.end - span.points.length * interval
        buf.push(
            ` ${span.period} secs ${fmtdate(start * 1000)} => ${fmtdate(span.end * 1000)} ${span.points.length} points`
        )
        for (let point of span.points) {
            buf.push(`     count ${point.count} = sum ${point.sum}`)
        }
    }
    print(buf.join('\n'))
}

const dumpQuery = function (metric: MetricQueryResult) {
    let points = metric.points.slice(0)
    let buf: string[] = []
    buf.push(
        `${metric.namespace}/${metric.metric}/${JSON.stringify(metric.dimensions)} ${metric.period} ${
            points.length
        } points`
    )
    for (let point of points) {
        buf.push(`     ${fmtdate(point.timestamp || 0)} = ${point.value || '-'} / ${point.count}`)
    }
    print(buf.join('\n'))
}

const print = (...args) => {
    console.log(...args)
}
globalThis.dt = dt
globalThis.fmtdate = fmtdate
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
