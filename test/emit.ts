/*
    emit.ts - Test basic emit functionality
 */
import {client, table, CustomMetrics, DefaultSpans, log} from './utils/init'

jest.setTimeout(7200 * 1000)

test('Test basic emit', async () => {
    let metrics = new CustomMetrics({client, table, owner: 'service', log: true})
    let timestamp = new Date(2000, 0, 1).getTime()
    let metric = await metrics.emit('test/emit', 'FirstMetric', 10, [], {timestamp})
    expect(metric).toBeDefined()
    expect(metric.namespace).toBe('test/emit')
    expect(metric.metric).toBe('FirstMetric')
    expect(metric.owner).toBe('service')
    expect(metric.spans.length).toBe(DefaultSpans.length)
    expect(metric.version).toBe(1)
    expect(metric.expires).toBeDefined()
    let span = metric.spans[0]
    let points = span.points
    expect(points.length).toBe(1)
    expect(span.samples).toBe(DefaultSpans[0].samples)
    expect(span.period).toBe(DefaultSpans[0].period)
    expect(points[0].count).toBe(1)
    expect(points[0].sum).toBe(10)
    expect(points[0].max).toBe(10)
    expect(points[0].min).toBe(10)

    /*
        Query to ensure results are committed
     */
    let r = await metrics.query('test/emit', 'FirstMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('FirstMetric')
    expect(r.namespace).toBe('test/emit')
    expect(r.period).toBe(300)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(r.samples)
    expect(r.points.at(-1)?.value).toBe(10)
    expect(r.points.at(-1)?.count).toBe(1)
    //  The last point bucket will end at timestamp + interval
    expect(r.points.at(-1)?.timestamp).toBe(timestamp + 30000)
})

test('Test emit with dimensions', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()

    await metrics.emit('test/emit', 'Launches', 10, [{}, {Rocket: 'SaturnV'}], {timestamp})
    await metrics.emit('test/emit', 'Launches', 10, [{}, {Rocket: 'Falcon9'}], {timestamp})

    /*
        Query total launches
     */
    let r = await metrics.query('test/emit', 'Launches', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.dimensions).toBeDefined()
    expect(Object.keys(r.dimensions).length).toBe(0)
    expect(r.points.length).toBe(r.samples)
    expect(r.points.at(-1)?.value).toBe(20)
    expect(r.points.at(-1)?.count).toBe(2)

    //  Query just one dimension
    r = await metrics.query('test/emit', 'Launches', {Rocket: 'Falcon9'}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.dimensions).toBeDefined()
    expect(r.dimensions.Rocket).toBe('Falcon9')
    expect(r.points.length).toBe(r.samples)
    expect(r.points.at(-1)?.value).toBe(10)
    expect(r.points.at(-1)?.count).toBe(1)

    //  Query unknown dimension
    r = await metrics.query('test/emit', 'Launches', {Rocket: 'Starship'}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.dimensions).toBeDefined()
    expect(r.dimensions.Rocket).toBe('Starship')
    expect(r.points.length).toBe(0)
})

test('Emit Series', async () => {
    let metrics = new CustomMetrics({client, table})
    let timestamp = new Date(2000, 0, 1).getTime()
    let span = DefaultSpans[0]
    let interval = span.period / span.samples

    let metric
    for (let i = 0; i < span.samples; i++) {
        metric = await metrics.emit('test/emit', 'SeriesMetric', i, [], {timestamp})
        metric = await metrics.emit('test/emit', 'SeriesMetric', i, [], {timestamp: timestamp + 1})
        timestamp += interval * 1000
    }
    let r = await metrics.query('test/emit', 'SeriesMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()
    expect(r.metric).toBe('SeriesMetric')
    expect(r.namespace).toBe('test/emit')
    expect(r.period).toBe(300)
    expect(r.points).toBeDefined()
    expect(r.points.length).toBe(10)
})

test('Emit API', async () => {
    let metrics = new CustomMetrics({client, table})
    expect(async () => {
        await metrics.emit('test/emit', 'Launches', null as any)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit('test/emit', 'Launches', undefined as any)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit('test/emit', 'Launches', 'invalid' as any)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit(null as any, 'Launches', 10)
    }).rejects.toThrow()
    expect(async () => {
        await metrics.emit('namespace', null as any, 10)
    }).rejects.toThrow()

    //  Emit with ttl
    await metrics.emit('test/emit', 'ShortLived', 10, [], {ttl: 3600})
})