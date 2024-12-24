/*
    buffer.ts - Test buffered emits
 */
import {client, table, CustomMetrics, DefaultSpans, log, dump, dumpMetric, dumpQuery} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test Buffer Basic', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    /*
        Buffer some metrics
     */
    for (let i = 0; i < 4; i++) {
        let metric = await metrics.emit('test/buffer', 'BasicMetric', 10, [], {buffer: {elapsed: 1800}, timestamp})
        expect(metric).toBeDefined()
        expect(metric.metric).toBe('BasicMetric')
        expect(metric.spans.length).toBe(1)
        expect(metric.spans[0].points.length).toBe(1)
        expect(metric.spans[0].points[0].count).toBe(i + 1)
        timestamp += interval * 1000
    }
    //  Query will flush
    let r = await metrics.query('test/buffer', 'BasicMetric', {}, 3600, 'avg', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('BasicMetric')
    expect(r.namespace).toBe('test/buffer')
    expect(r.period).toBe(3600)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points[11].value).toBe(10)
    expect(r.points[11].count).toBe(4)
})

test('Test elapsed buffers', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    /*
        Buffer some metrics and then flush
     */
    for (let i = 0; i < 4; i++) {
        let metric = await metrics.emit('test/buffer', 'ElapsedMetric', 1, [], {buffer: {elapsed: 1800}, timestamp})
        expect(metric).toBeDefined()
        expect(metric.metric).toBe('ElapsedMetric')
        expect(metric.spans.length).toBe(1)
        expect(metric.spans[0].points.length).toBe(1)
        expect(metric.spans[0].points[0].count).toBe(i + 1)
        timestamp += interval * 1000
    }
    /*
        Emit again after long delay this should cause the prior buffer to be flushed
     */
    timestamp += 3600 * 1000
    await metrics.emit('test/buffer', 'ElapsedMetric', 7, [], {buffer: {elapsed: 1800}, timestamp})

    let r = await metrics.query('test/buffer', 'ElapsedMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('ElapsedMetric')
    expect(r.namespace).toBe('test/buffer')
    expect(r.period).toBe(86400)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points[11].value).toBe(11)
    expect(r.points[11].count).toBe(5)
})

test('Test buffer API', async () => {
    let metrics = new CustomMetrics({client, table})
    let metric = await metrics.emit('test/buffer', 'CoverageMetric', 1, [], {buffer: {sum: 1}})
    expect(metric).toBeDefined()

    metrics = new CustomMetrics({client, table, buffer: {elapsed: 1800}})
    metric = await metrics.emit('test/buffer', 'CoverageMetric', 1, [])
    expect(metric).toBeDefined()

    metrics = new CustomMetrics({client, table, buffer: {elapsed: 1800}})
    metric = await metrics.emit('test/buffer', 'CoverageMetric', 1, [])
    expect(metric).toBeDefined()

    metrics = new CustomMetrics({client, table})
    metric = await metrics.emit('test/buffer', 'CoverageMetric', 1, [], {buffer: {count: 1}})
    metric = await metrics.emit('test/buffer', 'CoverageMetric', 1, [], {buffer: {count: 1}})
    expect(metric).toBeDefined()
})

test('Test stale buffered data', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()

    //  Emit a non-buffered metric and query
    let metric = await metrics.emit('test/buffer', 'StaleMetric', 7, [], {timestamp})
    expect(metric.spans[0].points[0].sum).toBe(7)
    let r = await metrics.query('test/buffer', 'StaleMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.points[11].value).toBe(7)

    /*
        Emit a stale buffered metric - should be discarded
     */
    timestamp -= 365 * 86400 * 1000
    await metrics.emit('test/buffer', 'StaleMetric', 100, [], {buffer: {elapsed: 1800, force: true}, timestamp})

    //  Result should be the original metric emitted
    timestamp += 365 * 86400 * 1000
    r = await metrics.query('test/buffer', 'StaleMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.points[11].value).toBe(7)
})

test('Buffered metric return', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let interval = 1

    for (let i = 0; i < 5; i++) {
        let metric = await metrics.emit('test/buffer', 'ReturnMetric', 1, [], {
            buffer: {sum: 5},
            timestamp,
        })
        if (i < 4) {
            expect(metric).toBeDefined()
            expect(metric.spans.length).toBe(1)
        } else {
            expect(metric.spans.length).toBe(6)
        }
        timestamp += interval * 1000
    }
})
