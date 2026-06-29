import { defineCollection } from "astro:content";
import { glob } from "astro/loaders";
import { z } from "astro/zod";

const docs = defineCollection({
  loader: glob({ base: "./src/content/docs", pattern: "**/*.md" }),
  schema: z.object({
    title: z.string(),
    description: z.string(),
    order: z.number(),
    category: z.enum(["Start Here", "Core Concepts", "Operator Workflows", "Reference"]),
    hero: z.string().optional(),
    summary: z.string().optional()
  })
});

export const collections = { docs };
