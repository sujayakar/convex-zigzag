import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

// The schema is entirely optional.
// You can delete this file (schema.ts) and the
// app will continue to work.
// The schema provides more precise TypeScript types.
export default defineSchema({
  messages: defineTable({
    author: v.string(),
    body: v.string(),
  }),

  toIndex: defineTable({
    a: v.string(),
    b: v.string(),
    c: v.string(),
  })
    .index("a", ["a"])
    .index("b", ["b"])
    .index("c", ["c"]),
});
