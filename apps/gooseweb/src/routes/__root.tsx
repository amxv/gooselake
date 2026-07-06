import type { ReactNode } from "react";
import {
  HeadContent,
  Link,
  Outlet,
  Scripts,
  createRootRoute
} from "@tanstack/react-router";
import appCss from "../styles/app.css?url";

export const Route = createRootRoute({
  head: () => ({
    meta: [
      { charSet: "utf-8" },
      { name: "viewport", content: "width=device-width, initial-scale=1" },
      { title: "Gooseweb" }
    ],
    links: [{ rel: "stylesheet", href: appCss }]
  }),
  component: RootComponent
});

function RootComponent() {
  return (
    <RootDocument>
      <main className="shell">
        <aside className="sidebar">
          <div className="brand">
            <h1>Gooseweb</h1>
            <span>Goosetower realtime console</span>
          </div>
          <nav className="nav" aria-label="Primary">
            <Link to="/" search={{}} activeOptions={{ exact: true }}>
              Realtime
            </Link>
          </nav>
        </aside>
        <section className="content">
          <Outlet />
        </section>
      </main>
    </RootDocument>
  );
}

function RootDocument({ children }: Readonly<{ children: ReactNode }>) {
  return (
    <html lang="en">
      <head>
        <HeadContent />
      </head>
      <body>
        {children}
        <Scripts />
      </body>
    </html>
  );
}
