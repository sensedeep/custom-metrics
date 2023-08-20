/*
    emit.ts - Test basic emit functionality
 */
import {Schema, Client, Table, CustomMetrics, DefaultSpans, dump, log} from './utils/init'

// jest.setTimeout(7200 * 1000)

const table = new Table({
    name: 'EmitTestTable',
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

test('Test basic emit', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let metric = await metrics.emit('myspace/test', 'FirstMetric', 10, [], {timestamp})
    expect(metric).toBeDefined()
    expect(metric.namespace).toBe('myspace/test')
    expect(metric.metric).toBe('FirstMetric')
    expect(metric.owner).toBe('service')
    expect(metric.spans.length).toBe(DefaultSpans.length)
    expect(metric.version).toBe(1)
    expect(metric.expires).toBeDefined()
    let span = metric.spans[0]
    let points = span.points
    expect(points.length).toBe(1)
    expect(span.samples).toBe(DefaultSpans[0].samples)
    expect(span.period).toBe(DefaultSpans[0].period)
    expect(points[0].count).toBe(1)
    expect(points[0].sum).toBe(10)
    expect(points[0].max).toBe(10)
    expect(points[0].min).toBe(10)

    /*
        Query to ensure results are committed
     */
    let r = await metrics.query('myspace/test', 'FirstMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('myspace/test')
    expect(r.period).toBe(300)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(1)
    expect(r.points[0].timestamp).toBe(timestamp)
})

test('Test emit with dimensions', async () => {
    let metrics = new CustomMetrics({onetable: table, owner: 'service', log: true})

    await metrics.emit('myspace/test', 'Launches', 10, [{}, {Rocket: 'SaturnV'}])
    await metrics.emit('myspace/test', 'Launches', 10, [{}, {Rocket: 'Falcon9'}])

    /*
        Query total launches
     */
    let r = await metrics.query('myspace/test', 'Launches', {}, 300, 'sum')
    expect(r).toBeDefined()
    expect(r.dimensions).toBeDefined()
    expect(Object.keys(r.dimensions).length).toBe(0)
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(20)
    expect(r.points[0].count).toBe(2)

    //  Query just one dimension
    r = await metrics.query('myspace/test', 'Launches', {Rocket: 'Falcon9'}, 300, 'sum')
    expect(r).toBeDefined()
    expect(r.dimensions).toBeDefined()
    expect(r.dimensions.Rocket).toBe('Falcon9')
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(1)

    //  Query unknown dimension
    r = await metrics.query('myspace/test', 'Launches', {Rocket: 'Starship'}, 300, 'sum')
    expect(r).toBeDefined()
    expect(r.dimensions).toBeDefined()
    expect(r.dimensions.Rocket).toBe('Starship')
    expect(r.points.length).toBe(0)
})

test('Emit API', async () => {
    let metrics = new CustomMetrics({onetable: table})
    expect(async () => {
        await metrics.emit('myspace/test', 'Launches', null as any)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit('myspace/test', 'Launches', undefined as any)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit('myspace/test', 'Launches', 'invalid' as any)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit(null as any, 'Launches', 10)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit('namespace', null as any, 10)
    }).rejects.toThrow()

    //  Emit with ttl
    await metrics.emit('myspace/test', 'ShortLived', 10, [], {ttl: 3600})
})

test('Destroy Table', async () => {
    await table.deleteTable('DeleteTableForever')
    expect(await table.exists()).toBe(false)
})
