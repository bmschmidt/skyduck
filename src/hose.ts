import {
  Collection,
  Commit,
  CommitCreate,
  CommitDelete,
  CommitEvent,
  CommitType,
  CommitUpdate,
  Jetstream,
} from '@skyware/jetstream';

import { promises as fs } from 'fs';
import {
  Data,
  Field,
  List,
  tableFromJSON,
  Timestamp,
  Utf8,
} from 'apache-arrow';
import { Database } from 'duckdb-async';
const jetstream = new Jetstream();
type RecordType = string;
import type { CommitCreateEvent, Records } from '@skyware/jetstream';
import { AppBskyFeedPost } from '@atcute/client/lexicons';

type RecordList<T extends keyof Records = keyof Records> = Record<
  T,
  CommitCreateEvent<T>[]
>;

const queue = {
  create: {} as RecordList,
  update: {} as Record<Collection, CommitUpdate<Collection>[]>,
  delete: {} as Record<Collection, CommitDelete<Collection>[]>,
};

// Listen for all commits, regardless of collection
jetstream.on('commit', (event) => {
  if (event.commit.operation === CommitType.Create) {
    if (
      queue['create'][event.commit.collection as keyof Records] === undefined
    ) {
      queue['create'][event.commit.collection as keyof Records] = [];
    }
    queue['create'][event.commit.collection as keyof Records].push(
      event as CommitCreateEvent<keyof Records>
    );
    // } else if (event.commit.operation === CommitType.Update) {
    //   // console.log('update in', event.commit.collection, event.commit.rkey);
    // } else if (event.commit.operation === CommitType.Delete) {
    //   // console.log('delete in', event.commit.collection, event.commit.rkey);
    // }
  }
});

const dbPromise = Database.create('test');

// const arrowTypes = {
//   text: new Utf8(),
//   langs: new List(new Field('lang', new Utf8(), true)),
//   createdAt: new Timestamp(1),
// };

const filehandles = new Map<string, fs.FileHandle>();

setInterval(async () => {
  // Most important thing is batches.
  const db = await dbPromise;
  for (const [k, v] of Object.entries(queue['create'])) {
    const coll = v;
    if (coll.length === 0) {
      continue;
    }
    const collectionName = coll[0].commit.collection;
    if (!filehandles.has(collectionName)) {
      filehandles.set(
        collectionName,
        await fs.open(`./${collectionName.split('.').join('_')}.jsonl`, 'w')
      );
    }
    const filehandle = filehandles.get(collectionName)!;
    for (const commit of coll) {
      const str = JSON.stringify(commit.commit.record) + '\n';
      await filehandle.write(str);
    }
  }
}, 500);

jetstream.start();
