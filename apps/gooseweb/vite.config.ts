import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";

export default defineConfig({
  resolve: {
    alias: {
      "~": fileURLToPath(new URL(".", import.meta.url))
    }
  },
  server: {
    port: 3001
  },
  plugins: [tailwindcss(), tanstackStart(), react()]
});
