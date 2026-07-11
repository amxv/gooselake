import { defineConfig } from "astro/config";
import zuedocs from "zuedocs/astro";

export default defineConfig({
  integrations: [zuedocs()],
  output: "static"
});
