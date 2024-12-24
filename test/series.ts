/*
    series.ts - Test query with series results
 */
import {client, table, CustomMetrics, DefaultSpans, dumpMetric} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test query with series', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[2]
    let interval = span.period / span.samples

    let metric
    for (let i = 0; i < 4; i++) {
        metric = await metrics.emit('test/series', 'FirstMetric', 10 * (i+1), [], {timestamp})
        timestamp += interval * 1000
    }
    timestamp -= (interval * 1000)
    
    let r = await metrics.query('test/series', 'FirstMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('test/series')
    expect(r.period).toBe(span.period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points[8].value).toBe(10)
    expect(r.points[8].count).toBe(1)
    expect(r.points[9].value).toBe(20)
    expect(r.points[10].value).toBe(30)
    expect(r.points[11].value).toBe(40)
})