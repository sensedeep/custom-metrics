/*
    gap.ts - Test gaps in metrics
 */
import {Client, Schema, CustomMetrics, DefaultSpans, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'GapTestTable'
const table = new Table({
    name: TableName,
    client: Client,
    partial: true,
    senselogs: log,
    schema: Schema,
})

test('Create Table', async () => {
    //  This will create a local table
    if (!(await table.exists())) {
        await table.createTable()
        expect(await table.exists()).toBe(true)
    }
})

test('Test gaps between emit and query', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    let metric
    for (let i = 0; i < span.samples * 20; i++) {
        metric = await metrics.emit('myspace/test', 'GapMetric', 10, [], {timestamp})
        timestamp += interval * 1000
    }
    expect(metric.spans[2].points.length).toBe(1)

    timestamp += 5 * 86400 * 1000
    let r = await metrics.query('myspace/test', 'GapMetric', {}, 86400, 'sum', {timestamp})
    expect(r.points.length).toBe(0)
})

test('Test data aging beyond highest span', async () => {
    const Spans = [{period: 3600, samples: 4}]
    let metrics = new CustomMetrics({onetable: table, owner: 'service2', log: false, spans: Spans})

    let timestamp = new Date(2000, 0, 1).getTime()
    let span = Spans[0]
    let interval = span.period / span.samples

    //  Emit more data than will fit in the span
    let metric
    for (let i = 0; i < span.samples * 2; i++) {
        metric = await metrics.emit('myspace/test', 'MyMetric', 1, [], {timestamp})
        timestamp += interval * 1000
    }
    expect(metric.spans[0].points.length).toBe(4)

    let r = await metrics.query('myspace/test', 'MyMetric', {}, 86400, 'sum', {timestamp, accumulate: true})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(4)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
