/*
    api.ts - Test misc api routines
 */
import {Schema, Client, CustomMetrics, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'APITestTable'
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

test('Test flush', async () => {
    let metrics = new CustomMetrics({onetable: table})
    await metrics.flush()
    await CustomMetrics.flushAll()
    await CustomMetrics.terminate()
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
