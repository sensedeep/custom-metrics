/*
    log.ts - Logging
 */
import {client, table, CustomMetrics, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Constructor without logging', async () => {
    let metrics = new CustomMetrics({client, table})

    let metric = await metrics.emit('test/log', 'FirstMetric', 10)
    expect(metric).toBeDefined()

    let r = await metrics.query('test/log', 'FirstMetric', {}, 300, 'sum')
    expect(r).toBeDefined()

    let list = await metrics.getMetricList('test/log')
    expect(list).toBeDefined()
})

test('Constructor with logging', async () => {
    let metrics = new CustomMetrics({client, table, log: true})

    let metric = await metrics.emit('test/log', 'FirstMetric', 10)
    expect(metric).toBeDefined()

    let r = await metrics.query('test/log', 'FirstMetric', {}, 300, 'sum')
    expect(r).toBeDefined()

    let list = await metrics.getMetricList('test/log')
    expect(list).toBeDefined()
})