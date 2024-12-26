/*
    query.ts - Test metric query
 */
import {client, table, CustomMetrics, DefaultSpans, dump, dumpMetric, print} from './utils/init'

jest.setTimeout(7200 * 1000)

test('Test basic query', async () => {
    let metrics = new CustomMetrics({client, table, log: false})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    for (let i = 0; i < 10; i++) {
        metric = await metrics.emit('test/query', 'BasicMetric', 7, [], {timestamp})
        timestamp += 30 * 1000
    }
    expect(metric.spans[0].points.length).toBe(10)

    let r = await metrics.query('test/query', 'BasicMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(10)
    expect(r.points.at(-1)?.value).toBe(7)
    expect(r.points.at(-1)?.count).toBe(1)
})

test('Test query period', async () => {
    let metrics = new CustomMetrics({client, table, log: false})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    for (let i = 0; i < 10; i++) {
        metric = await metrics.emit('test/query', 'PeriodMetric', 7, [], {timestamp})
        timestamp += 30 * 1000
    }
    timestamp -= 30 * 1000
    expect(metric.spans[0].points.length).toBe(10)

    //  With a period shorter than the lowest span - only one interval
    let r = await metrics.query('test/query', 'PeriodMetric', {}, 30, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[0].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(7)
    expect(r.points[0].count).toBe(1)

    //  With a period above the span emitted
    r = await metrics.query('test/query', 'PeriodMetric', {}, 3600, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(3600)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points[11].value).toBe(70)
    expect(r.points[11].count).toBe(10)

    //  With a period above the highest span
    let period = DefaultSpans.at(-1)!.period
    r = await metrics.query('test/query', 'PeriodMetric', {}, period + 1000, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points[11].value).toBe(70)
    expect(r.points[11].count).toBe(10)
})

test('Test query statistics', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    let metric
    for (let i = 0; i < 4; i++) {
        await metrics.emit('test/query', 'StatMetric', i, [], {timestamp})
        //  Emit a second data point 1ms after
        metric = await metrics.emit('test/query', 'StatMetric', i + 1, [], {timestamp: timestamp + 1})
        timestamp += interval * 1000
    }
    timestamp -= interval * 1000

    //  Average
    let r = await metrics.query('test/query', 'StatMetric', {}, 300, 'avg', {timestamp})
    expect(r.points.length).toBe(r.samples)
    expect(r.points[6].value).toBe(0.5)
    expect(r.points[6].count).toBe(2)
    expect(r.points[7].value).toBe(1.5)
    expect(r.points[7].count).toBe(2)
    expect(r.points[8].value).toBe(2.5)
    expect(r.points[8].count).toBe(2)
    expect(r.points[9].value).toBe(3.5)
    expect(r.points[9].count).toBe(2)

    //  Min
    r = await metrics.query('test/query', 'StatMetric', {}, 300, 'min', {timestamp})
    expect(r.points.length).toBe(r.samples)
    expect(r.points[0].value).toBe(0)
    expect(r.points[6].count).toBe(2)

    //  Max
    r = await metrics.query('test/query', 'StatMetric', {}, 300, 'max', {timestamp})
    expect(r.points.length).toBe(r.samples)
    expect(r.points[0].value).toBe(0)
    expect(r.points[9].value).toBe(4)
    expect(r.points[9].count).toBe(2)

    //  Count
    r = await metrics.query('test/query', 'StatMetric', {}, 300, 'count', {timestamp})
    expect(r.points.length).toBe(r.samples)
    expect(r.points[0].value).toBe(0)
    expect(r.points[9].count).toBe(2)
    expect(r.points[9].value).toBe(2)
})

test('Test query statistics with accumulate', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    let metric
    for (let i = 0; i < span.samples; i++) {
        // Multiple points
        metric = await metrics.emit('test/query', 'AccMetric', i, [], {timestamp})
        metric = await metrics.emit('test/query', 'AccMetric', i, [], {timestamp: timestamp + 1})
        timestamp += interval * 1000
    }
    timestamp -= interval * 1000

    //  Average
    let r = await metrics.query('test/query', 'AccMetric', {}, 300, 'avg', {accumulate: true, timestamp})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(4.5)
    expect(r.points[0].count).toBe(20)

    //  Min
    r = await metrics.query('test/query', 'AccMetric', {}, 300, 'min', {accumulate: true, timestamp})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(0)
    expect(r.points[0].count).toBe(20)

    //  Max
    r = await metrics.query('test/query', 'AccMetric', {}, 300, 'max', {accumulate: true, timestamp})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(9)
    expect(r.points[0].count).toBe(20)

    //  Count
    r = await metrics.query('test/query', 'AccMetric', {}, 300, 'count', {accumulate: true, timestamp})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(20)
    expect(r.points[0].count).toBe(20)
})

test('Test query p values', async () => {
    let metrics = new CustomMetrics({client, table, pResolution: 10})
    // let timestamp = new Date(2000, 0, 1).getTime()

    for (let i = 0; i < 10; i++) {
        await metrics.emit('test/query', 'PMetric', i, [])
    }
    //  p90
    let r = await metrics.query('test/query', 'PMetric', {}, 300, 'p90')
    expect(r.period).toBe(300)
    expect(r.points.length).toBe(r.samples)
    expect(r.points[9].value).toBe(9)
    expect(r.points[9].count).toBe(10)

    //  p50
    r = await metrics.query('test/query', 'PMetric', {}, 300, 'p50')
    expect(r.points.length).toBe(r.samples)
    expect(r.points[9].value).toBe(6)
    expect(r.points[9].count).toBe(10)
})

test('Test missing metrics', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime()

    //  Missing namespace
    let r = await metrics.query('Unknown', 'Unknown', {}, 300, 'avg', {timestamp})
    expect(r.points.length).toBe(0)

    //  Missing metric
    await metrics.emit('test/query', 'MMetric', 1, [], {timestamp})
    r = await metrics.query('test/query', 'Unknown', {}, 300, 'avg', {timestamp})
    expect(r.points.length).toBe(0)

    //  Missing span, but still return data point
    r = await metrics.query('test/query', 'MMetric', {}, 86400, 'avg', {timestamp})
    expect(r.points.length).toBe(r.samples)
})

test('Test query with non-standard period', async () => {
    let metrics = new CustomMetrics({client, table, log: false})

    let timestamp = new Date(2000, 0, 1).getTime()
    let metric
    for (let i = 0; i < 140; i++) {
        metric = await metrics.emit('test/query', 'BasicMetric', 7, [], {timestamp})
        timestamp += 30 * 1000
    }
    // timestamp -= 30 * 1000
    expect(metric.spans[0].points.length).toBe(10)

    /*
        Query 15 minutes
        This will return 3 points from the next span up (1 hr)
     */
    let r = await metrics.query('test/query', 'BasicMetric', {}, 900, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.period).toBe(DefaultSpans[1].period)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(3)
    expect(r.points[0].count).toBe(10)
    expect(r.points[0].value).toBe(70)
    expect(r.points[1].value).toBe(70)
    expect(r.points[2].value).toBe(70)
})