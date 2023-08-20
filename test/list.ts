/*
    list.ts - Get metric list
 */
import {Client, Schema, CustomMetrics, DefaultSpans, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'ListTestTable'
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

    let list = await metrics.getMetricList()
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(0)
    expect(list.metrics).toBeUndefined()
    expect(list.dimensions).toBeUndefined()

    //  Create some metrics
    await metrics.emit('myspace/test', 'Launches', 10, [{}, {Rocket: 'SaturnV'}])
    await metrics.emit('myspace/more', 'Crashes', 1, [{}, {Rocket: 'SaturnV'}])

    //  Should see two namespaces
    list = await metrics.getMetricList()
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(2)
    expect(list.namespaces.indexOf('myspace/test') >= 0).toBe(true)
    expect(list.namespaces.indexOf('myspace/more') >= 0).toBe(true)
    expect(list.metrics).toBeUndefined()
    expect(list.dimensions).toBeUndefined()

    //  Should see namespaces and metrics
    list = await metrics.getMetricList('myspace/test')
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(1)
    expect(list.namespaces[0]).toBe('myspace/test')
    expect(list.metrics).toBeDefined()
    expect(list.metrics!.length).toBe(1)
    expect(list.metrics![0]).toBe('Launches')
    expect(list.dimensions).toBeUndefined()

    //  Should see namespace, metrics and dimensions
    list = await metrics.getMetricList('myspace/test', 'Launches')
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(1)
    expect(list.namespaces[0]).toBe('myspace/test')
    expect(list.metrics).toBeDefined()
    expect(list.metrics!.length).toBe(1)
    expect(list.metrics![0]).toBe('Launches')
    expect(list.dimensions!.length).toBe(2)
    expect(JSON.stringify(list.dimensions![0])).toBe('{}')
    expect(JSON.stringify(list.dimensions![1])).toBe('{"Rocket":"SaturnV"}')
})

test('List API', async () => {
    //  With logging to pass through to OneTable find
    let metrics = new CustomMetrics({onetable: table})
    let list = await metrics.getMetricList('myspace/test', 'Launches', {log: false})
    expect(list).toBeDefined()
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
