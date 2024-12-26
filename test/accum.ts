/*
    accum.ts - Test accumulated queries
 */
import {client, table, CustomMetrics, DefaultSpans, log, dump} from './utils/init'

jest.setTimeout(7200 * 1000)

test('Test', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[2]
    let interval = span.period / span.samples

    for (let i = 0; i < 4; i++) {
        await metrics.emit('test/accum', 'FirstMetric', 10, [], {timestamp})
        timestamp += interval * 1000
    }
    // timestamp = timestamp - interval * 1000 + 1000
    let r = await metrics.query('test/accum', 'FirstMetric', {}, 86400, 'sum', {timestamp, accumulate: true})
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('test/accum')
    expect(r.period).toBe(span.period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(40)
    expect(r.points[0].count).toBe(4)
})