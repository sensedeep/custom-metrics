/*
    series.ts - Test query with series results
 */
import {Client, Schema, CustomMetrics, DefaultSpans, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'SeriesTestTable'
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
    let span = DefaultSpans[2]
    let interval = span.period / span.samples

    for (let i = 0; i < 4; i++) {
        await metrics.emit('myspace/test', 'FirstMetric', 10, [], {timestamp})
        timestamp += interval * 1000
    }
    timestamp = timestamp - (interval * 1000) + 1000
    let r = await metrics.query('myspace/test', 'FirstMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('myspace/test')
    expect(r.period).toBe(span.period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(4)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(1)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
