import { DynamoDBClient, QueryCommand } from '@aws-sdk/client-dynamodb';
type SpanDef = {
    period: number;
    samples: number;
};
export declare const DefaultSpans: SpanDef[];
export type Metric = {
    dimensions: string;
    expires?: number;
    id?: string;
    metric: string;
    namespace: string;
    owner?: string;
    version?: number;
    spans: Span[];
    seq?: number;
    _source?: string;
};
export type Point = {
    count: number;
    max?: number;
    min?: number;
    pvalues?: number[];
    sum: number;
    timestamp?: number;
};
export type Span = {
    end: number;
    period: number;
    samples: number;
    points: Point[];
};
export type MetricDimensions = {
    [key: string]: unknown;
};
export type MetricDimensionsList = MetricDimensions[];
export type MetricList = {
    namespaces: string[];
    metrics?: string[];
    dimensions?: MetricDimensions[];
};
export type MetricQueryPoint = {
    count: number;
    max?: number;
    min?: number;
    pvalues?: number[];
    sum?: number;
    timestamp?: number;
    value?: number;
};
export type MetricQueryResult = {
    dimensions: MetricDimensions;
    id?: string;
    metric: string;
    namespace: string;
    owner: string;
    period: number;
    points: MetricQueryPoint[];
    samples: number;
};
export type MetricOptions = {
    buffer?: MetricBufferOptions;
    client?: DynamoDBClient;
    consistent?: boolean;
    creds?: object;
    expires?: string;
    log?: true | 'verbose' | any;
    owner?: string;
    prefix?: string;
    pResolution?: number;
    primaryKey?: string;
    region?: string;
    sortKey?: string;
    source?: string;
    spans?: SpanDef[];
    table?: string;
    ttl?: number;
    type?: {
        [key: string]: string;
    };
};
export type MetricBufferOptions = {
    sum?: number;
    count?: number;
    elapsed?: number;
    force?: boolean;
};
export type MetricEmitOptions = {
    buffer?: MetricBufferOptions;
    log?: boolean;
    owner?: string;
    timestamp?: number;
    ttl?: number;
};
export type MetricListOptions = {
    log?: boolean;
    limit?: number;
    owner?: string;
};
export type MetricQueryOptions = {
    accumulate?: boolean;
    id?: string;
    log?: boolean;
    owner?: string;
    timestamp?: number;
};
type BufferElt = {
    count: number;
    dimensions: string;
    metric: string;
    namespace: string;
    sum: number;
    timestamp: number;
};
export declare class CustomMetrics {
    private consistent;
    private buffer;
    private buffers;
    private client;
    private expires;
    private log;
    private options;
    private owner;
    private prefix;
    private primaryKey;
    private sortKey;
    private pResolution;
    private source;
    private spans;
    private table;
    private type;
    private ttl;
    constructor(options?: MetricOptions);
    emit(namespace: string, metricName: string, value: number, dimensionsList?: MetricDimensionsList, options?: MetricEmitOptions): Promise<Metric>;
    private emitDimensions;
    bufferMetric(namespace: string, metricName: string, point: Point, dimensions: string, options: MetricEmitOptions): Promise<Metric>;
    private emitDimensionedMetric;
    static terminate(): Promise<void>;
    static flushAll(): Promise<void>;
    flush(): Promise<void>;
    flushElt(elt: BufferElt): Promise<void>;
    getBufferKey(namespace: string, metricName: string, dimensions: string): string;
    query(namespace: string, metricName: string, dimensions: MetricDimensions, period: number, statistic: string, options?: MetricQueryOptions): Promise<MetricQueryResult>;
    private accumulateMetric;
    private calculateSeries;
    private makeDimensionString;
    private makeDimensionObject;
    private addValue;
    private setPoint;
    getMetricList(namespace?: string, metric?: string, options?: MetricListOptions): Promise<MetricList>;
    private initMetric;
    getMetric(owner: string, namespace: string, metric: string, dimensions: string, log: boolean): Promise<Metric>;
    findMetrics(owner: string, namespace: string, metric: string | undefined, limit: number, startKey: object): Promise<{
        items: Metric[];
        next: object;
        command: QueryCommand;
    }>;
    putMetric(item: Metric, options: MetricEmitOptions): Promise<boolean>;
    mapItemFromDB(data: any): Metric;
    mapItemToDB(item: Metric): {
        [x: string]: string | number | {
            se: number;
            sp: number;
            ss: number;
            pt: any[];
        }[];
        spans: {
            se: number;
            sp: number;
            ss: number;
            pt: any[];
        }[];
        seq: number;
        _source: string;
    };
    static freeInstanceByKey(key: string): void;
    static saveInstance(tags: object, metrics: CustomMetrics): void;
    private getTimestamp;
    private assert;
    private info;
    private error;
    private trace;
    round(n: number): number;
    jitter(msecs: number): number;
    delay(time: number): Promise<boolean>;
}
export {};
