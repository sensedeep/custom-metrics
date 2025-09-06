/*
    aging.ts - Test aging spans in return values from emit

    Edit your test case here and invoke via: "jest debug"

    Or run VS Code in the top level directory and just run.
 */
import {client, table, CustomMetrics, log, dumpMetric, dumpQuery} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test unbuffered', async () => {
    let metrics = new CustomMetrics({client, table})

    let timestamp = Date.UTC(2000, 0, 1)

    // Emit value 10
    let metric = await metrics.emit('test/debug', 'Unbuffered', 10, [], {timestamp})
    expect(metric.spans[0].points.length).toBe(1)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(10)

    // Emit value 10 one interval later
    timestamp += 30 * 1000
    metric = await metrics.emit('test/debug', 'Unbuffered', 10, [], {timestamp})
    expect(metric.spans[0].points.length).toBe(2)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(10)
    expect(metric.spans[0].points[1].sum).toBe(10)

    /*
        Test that the data points are returned
     */
    let r = await metrics.query('test/debug', 'Unbuffered', {}, 86400, 'sum', {timestamp})
    expect(r.points.length).toBe(12)
    expect(r.points[11].value).toBe(20)
    expect(r.points[11].count).toBe(2)

    /*
        Emit another value 12 hours later to test the prior data points are returned
     */
    timestamp += 12 * 3600 * 1000
    metric = await metrics.emit('test/debug', 'Unbuffered', 100, [], {timestamp})
    expect(metric.spans[0].points.length).toBe(1)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(100)
    expect(metric.spans[2].points.length).toBe(1)
    expect(metric.spans[2].points[0].count).toBe(2)
    expect(metric.spans[2].points[0].sum).toBe(20)

    /*
        Query all results
     */
    r = await metrics.query('test/debug', 'Unbuffered', {}, 86400, 'sum', {timestamp})
    expect(r.points.length).toBe(12)
    expect(r.points[11].value).toBe(100)
    expect(r.points[11].count).toBe(1)
})


test('Test buffered', async () => {
    let metrics = new CustomMetrics({client, table})

    /*
        Emit two buffered data points
     */
    let timestamp = Date.UTC(2000, 0, 1)
    let metric = await metrics.emit('test/debug', 'Buffered', 10, [], {timestamp, buffer: {count: 10}})
    expect(metric.spans[0].points.length).toBe(1)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(10)

    timestamp += 60 * 1000
    metric = await metrics.emit('test/debug', 'Buffered', 10, [], {timestamp, buffer: {count: 10}})
    expect(metric.spans[0].points.length).toBe(2)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(10)
    expect(metric.spans[0].points[1].count).toBe(1)
    expect(metric.spans[0].points[1].sum).toBe(10)

    /*
        Test that the buffered data points are returned
     */
    timestamp += 60 * 1000
    let r = await metrics.query('test/debug', 'Buffered', {}, 86400, 'sum', {timestamp})
    expect(r.points.length).toBe(12)
    expect(r.points[11].value).toBe(20)
    expect(r.points[11].count).toBe(2)

    // Flush to persist
    await metrics.flush()

    /*
        Do with a new instance to force re-reading from storage
        Emit another buffered value to test the prior data points are returned
     */
    metrics = new CustomMetrics({client, table})
    timestamp += 12 * 3600 * 1000
    metric = await metrics.emit('test/debug', 'Buffered', 100, [], {timestamp, buffer: {count: 10}})
    expect(metric.spans[0].points.length).toBe(1)
    expect(metric.spans[0].points[0].count).toBe(1)
    expect(metric.spans[0].points[0].sum).toBe(100)
    expect(metric.spans[2].points.length).toBe(1)

    /*
        Query all results
     */
    r = await metrics.query('test/debug', 'Buffered', {}, 86400, 'sum', {timestamp})
    expect(r.points.length).toBe(12)
    expect(r.points[11].value).toBe(100)
    expect(r.points[11].count).toBe(1)
})