/*
    start.ts - Test metric query with start time
 */
import {client, table, CustomMetrics, DefaultSpans, dumpQuery, dump, dumpMetric} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test query with start', async () => {
    let metrics = new CustomMetrics({client, table, log: false})
    let timestamp = Date.UTC(2000, 0, 1)

    //  One week of data
    let metric
    for (let i = 0; i < 24 * 7; i++) {
        metric = await metrics.emit('test/query', 'StartMetric', 7, [], {timestamp})
        timestamp += 3600 * 1000
    }
    expect(metric.spans.length).toBe(6)

    // Check last 5 mins and last hour
    timestamp -= 3600 * 1000
    let r = await metrics.query('test/query', 'StartMetric', {}, 300, 'sum', {timestamp})
    expect(r.points.reduce((acc, point) => acc + point.count, 0)).toBe(1)
    r = await metrics.query('test/query', 'StartMetric', {}, 3600, 'sum', {timestamp})
    expect(r.points.reduce((acc, point) => acc + point.count, 0)).toBe(1)

    //  Get last day
    r = await metrics.query('test/query', 'StartMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[2].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(12)
    expect(r.points.reduce((acc, point) => acc + point.count, 0)).toBe(24)
    expect(r.points[0].count).toBe(2)

    /*
        Get last 2 days of data starting 4 days ago
        This is using the week span with 1/2 day intervals
     */
    let start = timestamp - 86400 * 4 * 1000
    r = await metrics.query('test/query', 'StartMetric', {}, 86400 * 2, 'sum', {start, timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[3].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(4)
    expect(r.points.at(-1)?.value).toBe(84)
    expect(r.points.at(-1)?.count).toBe(12)
})
