/*
    propagate.ts - Test emit will propagate from span to span
 */
import {client, table, CustomMetrics, DefaultSpans, dumpMetric} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = Date.UTC(2000, 0, 1)
    let span = DefaultSpans[2]
    let interval = span.period / span.samples

    for (let i = 0; i < 4; i++) {
        let metric = await metrics.emit('test/propagate', 'FirstMetric', i + 1, [], {timestamp})
        timestamp += interval * 1000
    }
    timestamp -= (interval * 1000)

    let r = await metrics.query('test/propagate', 'FirstMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('test/propagate')
    expect(r.period).toBe(span.period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points.reduce((acc, point) => acc + point?.count, 0)).toBe(4)
    expect(r.points.reduce((acc, point) => acc + point?.value, 0)).toBe(10)
})