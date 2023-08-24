declare const Version = 1;
declare const Schema: {
    format: string;
    version: string;
    indexes: {
        primary: {
            hash: string;
            sort: string;
        };
    };
    models: {
        readonly Metric: {
            readonly pk: {
                readonly type: "string";
                readonly value: "metric#${version}#${owner}";
            };
            readonly sk: {
                readonly type: "string";
                readonly value: "metric#${namespace}#${metric}#${dimensions}";
            };
            readonly dimensions: {
                readonly type: "string";
                readonly required: true;
                readonly encode: readonly ["sk", "#", "3"];
            };
            readonly expires: {
                readonly type: "date";
                readonly ttl: true;
            };
            readonly metric: {
                readonly type: "string";
                readonly required: true;
                readonly encode: readonly ["sk", "#", "2"];
            };
            readonly namespace: {
                readonly type: "string";
                readonly required: true;
                readonly encode: readonly ["sk", "#", "1"];
            };
            readonly owner: {
                readonly type: "string";
                readonly required: true;
                readonly encode: readonly ["pk", "#", "2"];
            };
            readonly version: {
                readonly type: "number";
                readonly default: 1;
                readonly encode: readonly ["pk", "#", "1"];
            };
            readonly id: {
                readonly type: "string";
            };
            readonly spans: {
                readonly type: "array";
                readonly required: true;
                readonly default: readonly [];
                readonly items: {
                    readonly type: "object";
                    readonly default: {};
                    readonly schema: {
                        readonly end: {
                            readonly type: "number";
                            readonly required: true;
                            readonly map: "se";
                        };
                        readonly period: {
                            readonly type: "number";
                            readonly required: true;
                            readonly map: "sp";
                        };
                        readonly samples: {
                            readonly type: "number";
                            readonly required: true;
                            readonly map: "ss";
                        };
                        readonly points: {
                            readonly type: "array";
                            readonly required: true;
                            readonly map: "pt";
                            readonly default: readonly [];
                            readonly items: {
                                readonly type: "object";
                                readonly schema: {
                                    readonly count: {
                                        readonly type: "number";
                                        readonly required: true;
                                        readonly map: "c";
                                    };
                                    readonly max: {
                                        readonly type: "number";
                                        readonly map: "x";
                                    };
                                    readonly min: {
                                        readonly type: "number";
                                        readonly map: "m";
                                    };
                                    readonly sum: {
                                        readonly type: "number";
                                        readonly required: true;
                                        readonly map: "s";
                                    };
                                    readonly pvalues: {
                                        readonly type: "array";
                                        readonly map: "v";
                                    };
                                    readonly timestamp: {
                                        readonly type: "number";
                                        readonly map: "e";
                                    };
                                };
                            };
                        };
                    };
                };
            };
            readonly seq: {
                readonly type: "number";
                readonly default: 0;
            };
            readonly _source: {
                readonly type: "string";
            };
        };
    };
    params: {
        isoDates: boolean;
        nulls: boolean;
        timestamps: boolean;
        typeField: string;
    };
};
export { Schema, Version };
