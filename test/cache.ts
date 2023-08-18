/*
    cache.ts - CustomMetrics instance cache
 */
import {Schema, Client, CustomMetrics, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'CacheTestTable'
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

test('Alloc', async () => {
    let tags = {name: 'primary'}
    let metrics = CustomMetrics.allocInstance(tags, {onetable: table, owner: 'service', log: true})
    expect(metrics).toBeDefined()
    expect(metrics instanceof CustomMetrics).toBe(true)
    expect(typeof metrics.emit == 'function').toBe(true)

    let inst = CustomMetrics.getInstance(tags)
    expect(inst).toEqual(metrics)

    let cache = CustomMetrics.getCache()
    expect(cache).toBeDefined()
    expect(Object.keys(cache).length).toBe(1)
    expect(cache[JSON.stringify(tags)]).toEqual(metrics)

    inst = CustomMetrics.allocInstance(tags)
    expect(inst).toEqual(metrics)
    cache = CustomMetrics.getCache()
    expect(Object.keys(cache).length).toBe(1)

    CustomMetrics.freeInstance(tags)
    inst = CustomMetrics.getInstance(tags)
    expect(inst).toBeUndefined()
    expect(cache).toBeDefined()
    expect(Object.keys(cache).length).toBe(0)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
