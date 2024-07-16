import { v } from "convex/values";
import { mutation, query } from "./_generated/server";
import {
  DocumentByInfo,
  GenericDataModel,
  GenericQueryCtx,
  NamedTableInfo,
  TableNamesInDataModel,
} from "convex/server";

export const send = mutation({
  args: { body: v.string(), author: v.string() },
  handler: async (ctx, { body, author }) => {
    const message = { body, author };
    await ctx.db.insert("messages", message);
  },
});

export const populate = mutation({
  handler: async (ctx) => {
    for (let i = 0; i < 100; i++) {
      await ctx.db.insert("toIndex", {
        a: (Math.random() * 10).toFixed(0),
        b: (Math.random() * 10).toFixed(0),
        c: (Math.random() * 10).toFixed(0),
      });
    }
  },
});

export const zigZag = query({
  args: {
    a: v.string(),
    b: v.string(),
    c: v.string(),

    maxResults: v.number(),
  },
  handler: async (ctx, args) => {
    const streamA = new IndexStream(ctx, "toIndex", "a", [
      { fieldName: "a", value: args.a },
    ]);
    const streamB = new IndexStream(ctx, "toIndex", "b", [
      { fieldName: "b", value: args.b },
    ]);
    const streamC = new IndexStream(ctx, "toIndex", "c", [
      { fieldName: "c", value: args.c },
    ]);
    const stream = new Intersection(new Union(streamA, streamB), streamC);
    console.time("execution time");
    const results = await stream.collect(args.maxResults);
    console.info(
      `${results.length} results for (a = "${args.a}" OR b = "${args.b}") AND c = "${args.c}"`,
    );
    const totalFetches =
      streamA.numFetches + streamB.numFetches + streamC.numFetches;
    const totalQueries =
      streamA.numQueries + streamB.numQueries + streamC.numQueries;
    console.info(`${totalFetches} fetches, ${totalQueries} queries`);
    console.timeEnd("execution time");
    return results;
  },
});

// TODO: We assume `_creationTime` is unique.
type CreationTime = number;

type Document = {
  _creationTime: CreationTime;
  _id: string;
};

abstract class Stream<T extends Document> {
  abstract next(): Promise<null | T>;
  abstract seek(position: CreationTime): void;

  async collect(maxItems?: number) {
    const results = [];
    while (!maxItems || results.length < maxItems) {
      const result = await this.next();
      if (!result) {
        break;
      }
      results.push(result);
    }
    return results;
  }
}

class Peekable<T extends Document, S extends Stream<T>> extends Stream<T> {
  private peeked: null | T = null;

  constructor(private stream: S) {
    super();
  }

  async next() {
    if (this.peeked) {
      const result = this.peeked;
      this.peeked = null;
      return result;
    }
    return await this.stream.next();
  }

  async peek() {
    if (!this.peeked) {
      this.peeked = await this.stream.next();
    }
    return this.peeked;
  }

  seek(position: CreationTime) {
    this.peeked = null;
    this.stream.seek(position);
  }
}

class Union<T extends Document> extends Stream<T> {
  left: Peekable<T, Stream<T>>;
  right: Peekable<T, Stream<T>>;

  constructor(left: Stream<T>, right: Stream<T>) {
    super();
    this.left = new Peekable(left);
    this.right = new Peekable(right);
  }

  async next(): Promise<null | T> {
    const left = await this.left.peek();
    const right = await this.right.peek();
    if (!left && !right) {
      return null;
    }
    if (!left) {
      return await this.right.next();
    }
    if (!right) {
      return await this.left.next();
    }

    if (left._creationTime === right._creationTime) {
      if (left._id !== right._id) {
        throw new Error("Inconsistent streams");
      }
      await this.left.next();
      await this.right.next();
      return left;
    }
    if (left._creationTime < right._creationTime) {
      return await this.left.next();
    } else {
      return await this.right.next();
    }
  }

  seek(position: CreationTime) {
    this.left.seek(position);
    this.right.seek(position);
  }
}

class Intersection<T extends Document> extends Stream<T> {
  constructor(
    private left: Stream<T>,
    private right: Stream<T>,
  ) {
    super();
  }

  async next(): Promise<null | T> {
    let left = await this.left.next();
    let right = await this.right.next();
    while (true) {
      if (!left || !right) {
        return null;
      }
      if (!left._creationTime || !right._creationTime) {
        throw new Error("Streams must have `_creationTime`");
      }
      if (left._creationTime === right._creationTime) {
        if (left._id !== right._id) {
          throw new Error("Inconsistent streams");
        }
        return left;
      }
      if (left._creationTime < right._creationTime) {
        this.left.seek(right._creationTime as number);
        left = await this.left.next();
      } else {
        this.right.seek(left._creationTime as number);
        right = await this.right.next();
      }
    }
  }

  async seek(position: number) {
    this.left.seek(position);
    this.right.seek(position);
  }
}

class IndexStream<
  DataModel extends GenericDataModel,
  TableName extends TableNamesInDataModel<DataModel>,
  IndexName extends keyof DataModel[TableName]["indexes"],
> extends Stream<DataModel[TableName]["document"] & Document> {
  position:
    | { type: "start" }
    | { type: "inclusive"; bound: number }
    | { type: "exclusive"; bound: number }
    | { type: "done" };

  iterator?: AsyncIterator<
    DocumentByInfo<NamedTableInfo<DataModel, TableName>>,
    any,
    undefined
  >;

  numFetches = 0;
  numQueries = 0;

  constructor(
    private ctx: GenericQueryCtx<DataModel>,
    private tableName: TableName,
    private indexName: IndexName,
    private indexPrefix: { fieldName: string; value: any }[],
  ) {
    super();
    this.position = { type: "start" };
  }

  async next(): Promise<null | (DataModel[TableName]["document"] & Document)> {
    if (this.position.type === "done") {
      return null;
    }
    let iterator = this.iterator;
    if (!iterator) {
      const query = this.ctx.db
        .query(this.tableName)
        .withIndex(this.indexName, (q) => {
          let builder: any = q;
          for (const { fieldName, value } of this.indexPrefix) {
            builder = builder.eq(fieldName, value);
          }
          if (this.position.type == "inclusive") {
            builder = builder.gte("_creationTime", this.position.bound);
          }
          if (this.position.type == "exclusive") {
            builder = builder.gt("_creationTime", this.position.bound);
          }
          return builder;
        });
      iterator = query[Symbol.asyncIterator]();
      this.iterator = iterator;
      this.numQueries++;
    }
    // TODO: We can cache this query for repeated `.next` calls.
    this.numFetches++;
    const result = await iterator.next();
    if (result.done) {
      this.position = { type: "done" };
      return null;
    }
    const document = result.value;
    this.position = {
      type: "exclusive",
      bound: document._creationTime as number,
    };
    return document as any;
  }

  seek(position: number) {
    if (this.position.type === "done") {
      return;
    }
    this.position = { type: "inclusive", bound: position };

    (this.iterator as any).closeQuery();
    this.iterator = undefined;
  }
}
