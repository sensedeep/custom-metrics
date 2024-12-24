/*
    pvalues.ts - Test emit and query with P-values
 */
import {client, table, CustomMetrics} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test emit with P-Values', async () => {
    let metrics = new CustomMetrics({client, table, pResolution: 10})
    for (let i = 0; i < 10; i++) {
        await metrics.emit('test/pvalues', 'PMetric', i)
    }
    //  p90
    let r = await metrics.query('test/pvalues', 'PMetric', {}, 300, 'p90')
    expect(r.points.length).toBe(r.samples)
    expect(r.points[9].value).toBe(9)
    expect(r.points[9].count).toBe(10)

    //  p50
    r = await metrics.query('test/pvalues', 'PMetric', {}, 300, 'p50')
    expect(r.points.length).toBe(r.samples)
    expect(r.points[9].value).toBe(6)
    expect(r.points[9].count).toBe(10)

    //  Accumulate
    r = await metrics.query('test/pvalues', 'PMetric', {}, 300, 'p50', {accumulate: true})
    expect(r.points.length).toBe(1)
    expect(r.points[0].value).toBe(6)
    expect(r.points[0].count).toBe(10)

    //  Higher span
    r = await metrics.query('test/pvalues', 'PMetric', {}, 86400, 'p90')
    expect(r.points.length).toBe(r.samples)
    expect(r.points[11].value).toBe(9)
    expect(r.points[11].count).toBe(10)
})