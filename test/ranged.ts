/*
    ranged.ts - Get a range of data
 */
import {client, table, CustomMetrics, DefaultSpans, dumpMetric, dumpQuery} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test that points before data and after data are filled', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime() + 10 * 3600 * 1000

    //  Use the year span
    let span = DefaultSpans[5]
    let interval = span.period / span.samples

    /*
        Emit one year worth of data
     */
    for (let i = 0; i < 12; i++) {
        await metrics.emit('test/gg', 'FillMetric', i + 1, [], {timestamp})
        timestamp += interval * 1000
    }
    /*
        Query 1/4 starting 7 months back
     */
    let period = span.period / 4
    let start = timestamp - (interval * 7 * 1000)
    let r = await metrics.query('test/gg', 'FillMetric', {}, period, 'sum', {start, timestamp})
    expect(r.points.length).toBe(3)
    expect(r.points[0].value).toBe(7)
    expect(r.points[1].value).toBe(8)
    expect(r.points[2].value).toBe(9)

    //  Query accumulate
    r = await metrics.query('test/gg', 'FillMetric', {}, period, 'sum', {start, timestamp, accumulate: true})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(24)
})
