/*
    query.ts - Test metric query
 */
import {Schema, Client, Table, CustomMetrics, DefaultSpans, dump, log} from './utils/init'

// jest.setTimeout(7200 * 1000)

const table = new Table({
    name: 'QueryTestTable',
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

test('Test basic query', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: false})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    for (let i = 0; i < 10; i++) {
        metric = await metrics.emit('myspace/test', 'QueryMetric', 7, [], {timestamp})
        timestamp += 30 * 1000
    }
    expect(metric.spans[0].points.length).toBe(10)

    let r = await metrics.query('myspace/test', 'QueryMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(10)
    expect(r.points[0].value).toBe(7)
    expect(r.points[0].count).toBe(1)
})

test('Test query period', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: false})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    for (let i = 0; i < 10; i++) {
        metric = await metrics.emit('myspace/test', 'PeriodMetric', 7, [], {timestamp})
        timestamp += 30 * 1000
    }
    expect(metric.spans[0].points.length).toBe(10)

    //  With a period shorter than the lowest span
    let r = await metrics.query('myspace/test', 'PeriodMetric', {}, 30, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(10)
    expect(r.points[0].value).toBe(7)
    expect(r.points[0].count).toBe(1)

    //  With a period above the span emitted
    r = await metrics.query('myspace/test', 'PeriodMetric', {}, 3600, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(3600)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(70)
    expect(r.points[0].count).toBe(10)

    //  With a period above the highest span
    let period = DefaultSpans.at(-1)!.period
    r = await metrics.query('myspace/test', 'PeriodMetric', {}, period + 1000, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(70)
    expect(r.points[0].count).toBe(10)
})

test('Test query statistics', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: false})
    for (let i = 0; i < 4; i++) {
        await metrics.emit('myspace/test', 'QueryMetric', i)
    }

    //  Average
    let r = await metrics.query('myspace/test', 'QueryMetric', {}, 300, 'avg')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(1.5)
    expect(r.points[0].count).toBe(4)

    //  Min
    r = await metrics.query('myspace/test', 'QueryMetric', {}, 300, 'min')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(0)
    expect(r.points[0].count).toBe(4)

    //  Max
    r = await metrics.query('myspace/test', 'QueryMetric', {}, 300, 'max')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(3)
    expect(r.points[0].count).toBe(4)

    //  Count
    r = await metrics.query('myspace/test', 'QueryMetric', {}, 300, 'count')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(4)
    expect(r.points[0].count).toBe(4)
})

test('Test query p values', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: false, pResolution: 10})
    for (let i = 0; i < 10; i++) {
        await metrics.emit('myspace/test', 'PMetric', i)
    }
    //  p90
    let r = await metrics.query('myspace/test', 'PMetric', {}, 300, 'p90')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(9)
    expect(r.points[0].count).toBe(10)

    //  p50
    r = await metrics.query('myspace/test', 'PMetric', {}, 300, 'p50')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(6)
    expect(r.points[0].count).toBe(10)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
