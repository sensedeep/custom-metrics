/*
    year.ts - Test emit functionality for one year
 */
import {client, table, CustomMetrics, DefaultSpans, log, dump, dumpMetric, dumpQuery} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test year span', async () => {
    let metrics = new CustomMetrics({client, table, owner: 'service', log: true})

    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[5]
    let interval = span.period / span.samples

    for (let i = 0; i < 12; i++) {
        let metric = await metrics.emit('test/year', 'FirstMetric', i + 1, [], {timestamp})
        // dumpMetric(metric)
        timestamp += interval * 1000
    }

    /*
        Expect results to be span bucket aligned. i.e. first point timestamp will be 00-02-24 and
        the last bucket will be partial 00-12-31
     */
    let r = await metrics.query('test/year', 'FirstMetric', {}, span.period, 'sum', {timestamp})

    dumpQuery(r)
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('test/year')
    expect(r.period).toBe(span.period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points.at(-2)?.value).toBe(12)
    expect(r.points.at(-2)?.count).toBe(1)
    expect(r.points.at(-1)?.timestamp).toBe(timestamp)
    expect(r.points.at(-1)?.count).toBe(0)
})