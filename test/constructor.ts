/*
    constructor.ts - test constructor options
 */
import {client, table, CustomMetrics} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Constructor with table name', async () => {
    let metrics = new CustomMetrics({table})
    expect(metrics).toBeDefined()
    expect(metrics instanceof CustomMetrics).toBe(true)
    expect(typeof metrics.emit == 'function').toBe(true)
})

test('Constructor with client', async () => {
    let metrics = new CustomMetrics({client, table})
    expect(metrics).toBeDefined()
    expect(metrics instanceof CustomMetrics).toBe(true)
    expect(typeof metrics.emit == 'function').toBe(true)

    let metric = await metrics.emit('test/cons', 'ClientMetric', 10)
    expect(metric).toBeDefined()
})

test('Constructor with custom spans', async () => {
    const Spans = [{period: 86400, samples: 24}]
    let metrics = new CustomMetrics({client, table, spans: Spans})

    let timestamp = Date.UTC(2000, 0, 1)
    let metric
    for (let i = 0; i < 26; i++) {
        metric = await metrics.emit('test/custom', 'CustomMetric', 10, [], {timestamp})
        timestamp += 3600 * 1000
    }
    expect(metric.spans[0].points.length).toBe(24)

    let r = await metrics.query('test/custom', 'CustomMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(86400)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(24)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(1)
})

test('Constructor with options', async () => {
    //  Log true
    let metrics = new CustomMetrics({table})
    expect(metrics).toBeDefined()

    //  Verbose log
    metrics = new CustomMetrics({table, log: 'verbose'})
    expect(metrics).toBeDefined()

    //  Custom log
    metrics = new CustomMetrics({table, log: {
        info: (message: string, context: {}) => null,
        error: (message: string, context: {}) => null,
    }})
    expect(metrics).toBeDefined()

    //  Verbose log
    metrics = new CustomMetrics({table, log: 'verbose'})
    expect(metrics).toBeDefined()

    //  DynamoDB prefix
    metrics = new CustomMetrics({table, prefix: 'met'})
    expect(metrics).toBeDefined()

    //  TTL
    metrics = new CustomMetrics({table, ttl: 86400})
    expect(metrics).toBeDefined()

    //  Consistent
    metrics = new CustomMetrics({table, consistent: true})
    expect(metrics).toBeDefined()

    expect(() => {
        //  empty spans
        new CustomMetrics({table, spans: []})
    }).toThrow()
    expect(() => {
        //  Invalid pResolution
        new CustomMetrics({table, pResolution: -1})
    }).toThrow()
    expect(() => {
        //  Invalid buffer
        new CustomMetrics({table, buffer: true as any})
    }).toThrow()
    expect(() => {
        //  Bad TTL
        new CustomMetrics({table, ttl: true as any})
    }).toThrow()
    expect(() => {
        //  Bad consistent
        new CustomMetrics({table, consistent: 42 as any})
    }).toThrow()
    expect(() => {
        //  Bad Source
        new CustomMetrics({table, source: true as any})
    }).toThrow()
    expect(() => {
        //  Missing database
        new CustomMetrics({})
    }).toThrow()
    expect(() => {
        //  Missing table name
        new CustomMetrics({client})
    }).toThrow()
    expect(() => {
        //  Missing options
        new CustomMetrics()
    }).toThrow()
})

test('Constructor coverage', async () => {
    new CustomMetrics({table, buffer: {sum: 100}})
    new CustomMetrics({table, source: 'internal'})
})