/*
    owner.ts - test owner scoping
 */
import {Schema, Client, CustomMetrics, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'OwnerTestTable'
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

test('Constructor with different owners', async () => {
    let m1 = new CustomMetrics({onetable: table, owner: 'app1', log: true})
    let m2 = new CustomMetrics({onetable: table, owner: 'service2', log: true})

    //  These should not clash
    await m1.emit('myspace/test', 'Launches', 5)
    await m2.emit('myspace/test', 'Launches', 10)

    let r1 = await m1.query('myspace/test', 'Launches', {}, 86400, 'sum')
    expect(r1).toBeDefined()
    expect(r1.owner).toBe('app1')
    expect(r1.points.length).toBe(1)
    expect(r1.points[0].value).toBe(5)

    let r2 = await m2.query('myspace/test', 'Launches', {}, 86400, 'sum')
    expect(r2).toBeDefined()
    expect(r2.owner).toBe('service2')
    expect(r2.points.length).toBe(1)
    expect(r2.points[0].value).toBe(10)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
