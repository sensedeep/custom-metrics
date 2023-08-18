/*
    constructor.ts - test constructor options
    
        buffer?: MetricBufferOptions
        client?: object
        log?: true | any
        onetable?: Table<OneSchema>
        owner?: string
        primaryKey?: string
        sortKey?: string
        prefix?: string
        pResolution?: number
        source?: string
        spans?: SpanDef[]
        tableName?: string
        typeField?: string
        ttl?: number
 */
import {Schema, Client, CustomMetrics, log, Table, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

const TableName = 'ConstructorTestTable'
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
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: true})
    expect(metrics).toBeDefined()
    expect(metrics instanceof CustomMetrics).toBe(true)
    expect(typeof metrics.emit == 'function').toBe(true)
})

test('Constructor with client', async () => {
    let metrics = new CustomMetrics({client: Client, tableName: TableName, owner: 'service', log: false})
    expect(metrics).toBeDefined()
    expect(metrics instanceof CustomMetrics).toBe(true)
    expect(typeof metrics.emit == 'function').toBe(true)
})

test('Constructor with custom spans', async () => {
    const Spans = [{period: 86400, samples: 24}]
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: false, spans: Spans})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    for (let i = 0; i < 26; i++) {
        metric = await metrics.emit('myspace/test', 'ConsMetric', 10, [], {timestamp})
        timestamp += 3600 * 1000
    }
    expect(metric.spans[0].points.length).toBe(24)

    let r = await metrics.query('myspace/test', 'ConsMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(86400)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(24)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(1)
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
