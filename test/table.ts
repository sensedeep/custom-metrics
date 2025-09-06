/*
    table.ts - Test get metric table of dimensions
 */
import {client, table, CustomMetrics, log, dump} from './utils/init'

// jest.setTimeout(7200 * 1000)

test('Test get metric table', async () => {
    let metrics = new CustomMetrics({client, table, log: true})
    let timestamp = Date.UTC(2000, 0, 1)

    let metric = await metrics.emit('test/table', 'Temp', 10, [], {timestamp})
    metric = await metrics.emit('test/table', 'Temp', 10, [{}, {Rocket:'SaturnV'}], {timestamp})
    metric = await metrics.emit('test/table', 'Temp', 10, [{}, {Rocket:'Falcon9'}], {timestamp})

    let list = await metrics.queryMetrics('test/table', 'Temp', 300, 'sum', {timestamp})
    expect(list).toBeDefined()
    expect(list.length).toBe(3)
    expect(list[0].dimensions).toEqual({})
    expect(list[0].points.length).toBe(1)
    expect(list[0].points[0].value).toBe(30)

    expect(list[1].dimensions).toEqual({Rocket: 'Falcon9'})
    expect(list[1].points.length).toBe(1)
    expect(list[1].points[0].value).toBe(10)
})