/*
    basic.ts - base operations: emit / query
 */
import {client, table, CustomMetrics, log, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Basic test harness', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = new Date(2000, 0, 1).getTime()

    //  This first emit will initialize the metric
    let metric = await metrics.emit('test/basic', 'FirstMetric', 10, [], {timestamp})
    expect(metric).toBeDefined()

    //  The second emit will read the metric and update
    metric = await metrics.emit('test/basic', 'FirstMetric', 10, [{}, {Rocket:'SaturnV'}], {timestamp})

    let r = await metrics.query('test/basic', 'FirstMetric', {}, 300, 'sum', {timestamp})
    expect(r).toBeDefined()

    let list = await metrics.getMetricList('test/basic')
    expect(list).toBeDefined()

    list = await metrics.getMetricList('test/basic', 'FirstMetric')
    expect(list).toBeDefined()
})