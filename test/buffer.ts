/*
    buffer.ts - Test buffered emits
 */
import {Client, Schema, CustomMetrics, DefaultSpans, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'BufferTestTable'
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

test('Test', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    /*
        Buffer some metrics and then flush
     */
    for (let i = 0; i < 4; i++) {
        let metric = await metrics.emit('myspace/test', 'BufferMetric', 10, [], {buffer: {elapsed: 1800}, timestamp})
        expect(metric).toBeDefined()
        expect(metric.metric).toBe('BufferMetric')
        expect(metric.spans.length).toBe(1)
        expect(metric.spans[0].points.length).toBe(1)
        expect(metric.spans[0].points[0].count).toBe(i + 1)
        timestamp += interval * 1000
    }
    let r = await metrics.query('myspace/test', 'BufferMetric', {}, 3600, 'avg', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('BufferMetric')
    expect(r.namespace).toBe('myspace/test')
    expect(r.period).toBe(3600)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(4)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
