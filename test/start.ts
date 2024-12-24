/*
    start.ts - Test metric query with start time
 */
import {client, table, CustomMetrics, DefaultSpans} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test query with start', async () => {
    let metrics = new CustomMetrics({client, table, log: false})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    //  One week of data
    for (let i = 0; i < 24 * 7; i++) {
        metric = await metrics.emit('test/query', 'StartMetric', 7, [], {timestamp})
        timestamp += 3600 * 1000
    }
    expect(metric.spans.length).toBe(6)
    expect(metric.spans.at(0).points.length).toBe(1)
    expect(metric.spans.at(1).points.length).toBe(1)
    expect(metric.spans.at(2).points.length).toBe(12)
    expect(metric.spans.at(3).points.length).toBe(12)

    //  Get last day
    let r = await metrics.query('test/query', 'StartMetric', {}, 86400, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[2].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(12)
    expect(r.points[0].value).toBe(14)
    expect(r.points[0].count).toBe(2)

    //  Get last 2 days of data starting 4 days ago
    let start = timestamp - 86400 * 4 * 1000
    r = await metrics.query('test/query', 'StartMetric', {}, 86400 * 2, 'sum', {start, timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[3].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points.at(-1)?.value).toBe(84)
    expect(r.points.at(-1)?.count).toBe(12)
})
