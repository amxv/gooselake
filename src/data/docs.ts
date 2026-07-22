export const siteConfig = {
  name: "Gooselake",
  strapline: "Agent runtime control tower",
  description:
    "Gooselake is a machine-side runtime for durable agent sessions, provider abstraction, streaming control, and real execution.",
  repoUrl: "https://github.com/amxv/gooselake",
  accentColor: "#0f766e",
  accentColorDark: "#5eead4",
  footerSections: [
    {
      title: "Gooselake",
      text:
        "A machine-side runtime for durable agent sessions, streaming control, and real execution on real hosts."
    },
    {
      title: "What this site covers",
      text:
        "A practical operating manual: the mental model, local setup, runtime services, client-building guidance, deployment, and reference material."
    },
    {
      title: "Author",
      linkPrefix: "Made by ",
      linkHref: "https://ashray.xyz",
      linkLabel: "ashray.xyz"
    }
  ]
} as const;

export const docCategories = [
  "Start Here",
  "Mental Model",
  "Runtime Services",
  "Client Builders",
  "Operators",
  "Reference"
] as const;

export const primaryNav = [
  { href: "/docs", label: "Docs" },
  { href: siteConfig.repoUrl, label: "GitHub", external: true }
];
