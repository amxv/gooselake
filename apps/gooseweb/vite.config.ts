import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import { goosetowerHttpTarget } from "./app/realtime/goosetower-target";

const appRoot = fileURLToPath(new URL("./", import.meta.url));
const defaultGoosetowerUrl = "ws://127.0.0.1:8090/v1/realtime";

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
    port: 3000,
    strictPort: true,
    proxy: {
      "/api/dev-ticket": {
        target: goosetowerHttpTarget(
          process.env.VITE_GOOSETOWER_URL ?? defaultGoosetowerUrl,
          process.env.VITE_GOOSETOWER_HTTP_URL
        ),
        changeOrigin: true,
        rewrite: () => "/v1/dev/tickets",
        headers: {
          Authorization: "Bearer dev-goosetower-token"
        }
      }
    }
  },
  plugins: [tailwindcss(), tanstackStart(), react()]
});
