/*
    buffer.ts - Test buffered emits
 */
import {client, table, CustomMetrics, DefaultSpans, log, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    /*
        Buffer some metrics and then flush
     */
    for (let i = 0; i < 4; i++) {
        let metric = await metrics.emit('test/buffer', 'BufferMetric', 10, [], {buffer: {elapsed: 1800}, timestamp})
        expect(metric).toBeDefined()
        expect(metric.metric).toBe('BufferMetric')
        expect(metric.spans.length).toBe(1)
        expect(metric.spans[0].points.length).toBe(1)
        expect(metric.spans[0].points[0].count).toBe(i + 1)
        timestamp += interval * 1000
    }
    let r = await metrics.query('test/buffer', 'BufferMetric', {}, 3600, 'avg', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('BufferMetric')
    expect(r.namespace).toBe('test/buffer')
    expect(r.period).toBe(3600)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(10)
    expect(r.points[0].count).toBe(4)
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
        let metric = await metrics.emit('test/buffer', 'BufferMetric', 1, [], {buffer: {elapsed: 1800}, timestamp})
        expect(metric).toBeDefined()
        expect(metric.metric).toBe('BufferMetric')
        expect(metric.spans.length).toBe(1)
        expect(metric.spans[0].points.length).toBe(1)
        expect(metric.spans[0].points[0].count).toBe(i + 1)
        timestamp += interval * 1000
    }
    /*
        Emit again after long delay this should cause the prior buffer to be flushed
     */
    timestamp += 86400 * 1000
    await metrics.emit('test/buffer', 'BufferMetric', 7, [], {buffer: {elapsed: 1800}, timestamp})

    let r = await metrics.query('test/buffer', 'BufferMetric', {}, 3600, 'avg', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('BufferMetric')
    expect(r.namespace).toBe('test/buffer')
    expect(r.period).toBe(3600)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(2.2)
    expect(r.points[0].count).toBe(5)
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