/*
    current.ts - TEst current query
 */
import {client, table, CustomMetrics, DefaultSpans, log, dump} from './utils/init'

jest.setTimeout(7200 * 1000)

test('Current test harness', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]

    /*
        Current statistic should return only the last point, which may include multiple data items
        if emitted inside the span interval.
     */
    //  Emit two points inside the span interval period
    await metrics.emit('test/current', 'FirstMetric', 2, [], {timestamp})
    timestamp += 1000
    await metrics.emit('test/current', 'FirstMetric', 4, [], {timestamp})

    let r = await metrics.query('test/current', 'FirstMetric', {}, 300, 'current', {timestamp, accumulate: true})
    expect(r).toBeDefined()
    expect(r.period).toBe(span.period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(3)
    expect(r.points[0].count).toBe(2)

    //  Move to a new hour to expire the earlier points from the lowest span
    timestamp += 3600 * 1000

    /*
        Emit two metrics outside the span interval. The query should only return the most recent one.
     */
    //  Emit two metrics outside the span interval. 
    await metrics.emit('test/current', 'FirstMetric', 2, [], {timestamp})
    timestamp += (span.period / span.samples + 1) * 1000
    await metrics.emit('test/current', 'FirstMetric', 4, [], {timestamp})

    r = await metrics.query('test/current', 'FirstMetric', {}, 300, 'current', {timestamp, accumulate: true})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(4)
    expect(r.points[0].count).toBe(1)

    /*
        Move to the next interval. Query should the sample all prior intervals if the last inverval is empty.
        Test the last interval is empty by doing non-accumulated query.
     */
    timestamp += span.period / span.samples * 1000
    r = await metrics.query('test/current', 'FirstMetric', {}, 300, 'current', {timestamp, accumulate: false})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points.length).toBe(10)
    expect(r.points[9].count).toBe(0)

    //  Now test accumulated query.
    r = await metrics.query('test/current', 'FirstMetric', {}, 300, 'current', {timestamp, accumulate: true})
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points.length).toBe(1)
    expect(r.points[0].count).toBe(1)
    expect(r.points[0].value).toBe(4)

    //  When using a higher span, still should get the same result.
    r = await metrics.query('test/current', 'FirstMetric', {}, 86400, 'current', {timestamp, accumulate: true})
    expect(r.points.length).toBe(1)
    expect(r.points[0].count).toBe(1)
    expect(r.points[0].value).toBe(4)
})