/*
    pvalues.ts - Test emit and query with P-values
 */
import {Schema, Client, Table, CustomMetrics, DefaultSpans, dump, log} from './utils/init'

// jest.setTimeout(7200 * 1000)

const table = new Table({
    name: 'PTestTable',
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

test('Test emit with P-Values', async () => {
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

    //  Accumulate
    r = await metrics.query('myspace/test', 'PMetric', {}, 300, 'p50', {accumulate: true})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(6)
    expect(r.points[0].count).toBe(10)

    //  Higher span
    r = await metrics.query('myspace/test', 'PMetric', {}, 86400, 'p90')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(9)
    expect(r.points[0].count).toBe(10)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
