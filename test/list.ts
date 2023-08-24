/*
    list.ts - Get metric list
 */
import {client, table, CustomMetrics, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test get metric list', async () => {
    /*
        Must have unique owner to isolate own namespaces
     */
    let metrics = new CustomMetrics({client, table, owner: 'list', log: true})

    let list = await metrics.getMetricList()
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(0)
    expect(list.metrics).toBeUndefined()
    expect(list.dimensions).toBeUndefined()

    //  Create some metrics
    await metrics.emit('test/list', 'Launches', 10, [{}, {Rocket: 'SaturnV'}])
    await metrics.emit('test/another-list', 'Crashes', 1, [{}, {Rocket: 'SaturnV'}])

    //  Should see two namespaces
    list = await metrics.getMetricList()
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(2)
    expect(list.namespaces.indexOf('test/list') >= 0).toBe(true)
    expect(list.namespaces.indexOf('test/another-list') >= 0).toBe(true)
    expect(list.metrics).toBeUndefined()
    expect(list.dimensions).toBeUndefined()

    //  Should see namespaces and metrics
    list = await metrics.getMetricList('test/list')
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(1)
    expect(list.namespaces[0]).toBe('test/list')
    expect(list.metrics).toBeDefined()
    expect(list.metrics!.length).toBe(1)
    expect(list.metrics![0]).toBe('Launches')
    expect(list.dimensions).toBeUndefined()

    //  Should see namespace, metrics and dimensions
    list = await metrics.getMetricList('test/list', 'Launches')
    expect(list).toBeDefined()
    expect(list.namespaces).toBeDefined()
    expect(list.namespaces.length).toBe(1)
    expect(list.namespaces[0]).toBe('test/list')
    expect(list.metrics).toBeDefined()
    expect(list.metrics!.length).toBe(1)
    expect(list.metrics![0]).toBe('Launches')
    expect(list.dimensions!.length).toBe(2)
    expect(JSON.stringify(list.dimensions![0])).toBe('{}')
    expect(JSON.stringify(list.dimensions![1])).toBe('{"Rocket":"SaturnV"}')
})

test('List API', async () => {
    //  With logging to pass through to OneTable find
    let metrics = new CustomMetrics({client, table})
    let list = await metrics.getMetricList('test/list', 'Launches', {log: false})
    expect(list).toBeDefined()
})