import { Event } from '../model/event';
import { Stream } from '../model/stream';
import { PersistenceProvider } from './provider';

/**
 * A Persistence Provider that handle all the data in memory. It is a very simple implementation that should be used
 * only for development and test purposes.
 */
export class InMemoryProvider implements PersistenceProvider {
    private store: Map<string, Map<string, Array<Event>>> = new Map();

    public async addEvent(stream: Stream, data: any, type = '') {
        const currentEvents = await this.getEventsList(stream.aggregation, stream.id);
        const event: Event = {
            commitTimestamp: new Date().getTime(),
            eventType: type,
            payload: data,
            sequence: currentEvents.length,
        };
        currentEvents.push(event);
        return event;
    }

    public async getEvents(stream: Stream, offset = 0, limit?: number) {
        const history = this.getEventsList(stream.aggregation, stream.id);
        return this.take(history.slice(offset), limit || history.length);
    }

    public async getAggregations(offset = 0, limit?: number): Promise<Array<string>> {
        const keys = Array.from(this.store.keys());

        return this.take(keys.sort().slice(offset), (limit || this.store.size));
    }

    private take(array: any[], n: number = 1) {
        return array.slice(0, n < 0 ? 0 : n)
    }

    public async getStreams(aggregation: string, offset = 0, limit?: number): Promise<Array<string>> {
        const streams = this.store.get(aggregation);
        if (streams) {
            const keys = Array.from(streams.keys());
            return this.take(keys.sort().slice(offset), limit || this.store.size);
        }
        return [];
    }

    private getEventsList(aggregation: string, streamId: string) {
        let streams = this.store.get(aggregation);
        if (!streams) {
            streams = new Map<string, Array<Event>>();
            this.store.set(aggregation, streams);
        }
        let history = streams.get(streamId);
        if (!history) {
            history = new Array<Event>();
            streams.set(streamId, history);
        }
        return history;
    }
}
