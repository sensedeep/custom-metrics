/*
    gap.ts - Test gaps in metrics
 */
import {client, table, CustomMetrics, DefaultSpans} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test gaps between emit and query', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    let metric
    for (let i = 0; i < span.samples * 20; i++) {
        metric = await metrics.emit('test/gap', 'GapMetric', 10, [], {timestamp})
        timestamp += interval * 1000
    }
    expect(metric.spans[2].points.length).toBe(1)

    timestamp += 5 * 86400 * 1000
    let r = await metrics.query('test/gap', 'GapMetric', {}, 86400, 'sum', {timestamp})
    expect(r.points.length).toBe(0)
})

test('Test data aging beyond highest span', async () => {
    const Spans = [{period: 3600, samples: 4}]
    let metrics = new CustomMetrics({client, table, owner: 'service2', log: false, spans: Spans})

    let timestamp = new Date(2000, 0, 1).getTime()
    let span = Spans[0]
    let interval = span.period / span.samples

    //  Emit more data than will fit in the span
    let metric
    for (let i = 0; i < span.samples * 2; i++) {
        metric = await metrics.emit('test/gap', 'MyMetric', 1, [], {timestamp})
        timestamp += interval * 1000
    }
    expect(metric.spans[0].points.length).toBe(4)

    let r = await metrics.query('test/gap', 'MyMetric', {}, 86400, 'sum', {timestamp, accumulate: true})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(4)
})


test('Test predated data', async() => {
    let metrics = new CustomMetrics({client, table})

    //  Get at least one point in the span
    let timestamp = new Date(2000, 0, 1).getTime()
    let metric = await metrics.emit('test/gap', 'PreDate', 1, [], {timestamp})
    expect(metric).toBeDefined()

    //  This emit should be discarded as it is too early
    timestamp -= 365 * 86400 * 1000
    metric = await metrics.emit('test/gap', 'PreDate', 1, [], {timestamp})
    expect(metric).toBeDefined()
    expect(metric.spans[0].points.length).toBe(1)
    expect(metric.spans[5].points.length).toBe(0)

    let r = await metrics.query('test/gap', 'PreDate', {}, 365 * 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(1)
    expect(r.points[0].count).toBe(1)
})