/*
    basic.ts - base emit / query
 */
import {Schema, Client, CustomMetrics, log, Table, dump} from './utils/init'

jest.setTimeout(7200 * 1000)

const TableName = 'BasicTestTable'
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

test('Constructor with OneTable', async () => {
    let metrics = new CustomMetrics({onetable: table, log: true})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric = await metrics.emit('myspace/test', 'FirstMetric', 10, [], {timestamp})
    expect(metric).toBeDefined()

    let r = await metrics.query('myspace/test', 'FirstMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()

    let list = await metrics.getMetricList('myspace/test', 'FirstMetric')
    expect(list).toBeDefined()
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
