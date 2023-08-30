/*
    schema.ts -- CustomMetrics Schema
 */
const Version = 1

const Schema = {
    format: 'onetable:1.1.0',
    version: '0.0.1',
    indexes: {primary: {hash: 'pk', sort: 'sk'}},
    models: {
        Metric: {
            pk: {type: 'string', value: 'metric#${version}#${owner}'},
            sk: {type: 'string', value: 'metric#${namespace}#${metric}#${dimensions}'},
            dimensions: {type: 'string', required: true, encode: ['sk', '#', '3']},
            expires: {type: 'date', ttl: true},
            metric: {type: 'string', required: true, encode: ['sk', '#', '2']},
            namespace: {type: 'string', required: true, encode: ['sk', '#', '1']},
            owner: {type: 'string', required: true, encode: ['pk', '#', '2']},
            version: {type: 'number', default: Version, encode: ['pk', '#', '1']},
            id: {type: 'string' },      // Never stored. Preserved on query
            spans: {
                type: 'array',
                required: true,
                default: [],
                items: {
                    type: 'object',
                    default: {},
                    schema: {
                        // When the points will be full.
                        end: {type: 'number', required: true, map: 'se'}, 
                        period: {type: 'number', required: true, map: 'sp'},
                        samples: {type: 'number', required: true, map: 'ss'},
                        points: {
                            type: 'array',
                            required: true,
                            map: 'pt',
                            default: [],
                            items: {
                                type: 'object',
                                schema: {
                                    count: {type: 'number', required: true, map: 'c'},
                                    max: {type: 'number', map: 'x'},
                                    min: {type: 'number', map: 'm'},
                                    sum: {type: 'number', required: true, map: 's'},

                                    //  Never stored
                                    pvalues: {type: 'array', map: 'v'},
                                    timestamp: {type: 'number', map: 'e', },
                                },
                            },
                        },
                    },
                },
            },
            seq: {type: 'number', default: 0},
            _source: {type: 'string'}, // When set, bypass DynamoDB steams change detection
        }
    } as const,
    params: {
        isoDates: false,
        nulls: false,
        timestamps: false,
        typeField: '_type',
    },
}

export {Schema, Version}