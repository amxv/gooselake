import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

const appRoot = fileURLToPath(new URL("./", import.meta.url));

export default defineConfig({
  resolve: {
    alias: {
      "~": appRoot,
      "~/app": fileURLToPath(new URL("./app", import.meta.url)),
      "~/components": fileURLToPath(new URL("./components", import.meta.url)),
      "~/hooks": fileURLToPath(new URL("./hooks", import.meta.url)),
      "~/lib": fileURLToPath(new URL("./lib", import.meta.url)),
      "~/src": fileURLToPath(new URL("./src", import.meta.url))
    }
  },
  server: {
    port: 3001
  },
  plugins: [tailwindcss(), tanstackStart(), react()]
});
