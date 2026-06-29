export const siteConfig = {
  name: "Gooselake",
  strapline: "Agent runtime control tower",
  description:
    "Gooselake is a machine-side runtime for durable agent sessions, provider abstraction, streaming control, and real execution.",
  repoUrl: "https://github.com/amxv/gooselake",
  footerSections: [
    {
      title: "Gooselake",
      text:
        "A machine-side runtime for durable agent sessions, streaming control, and real execution on real hosts."
    },
    {
      title: "What this site covers",
      text:
        "Product framing, getting started steps, core concepts, and the operator workflows that matter once an agent graduates from demo mode."
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
  "Core Concepts",
  "Operator Workflows",
  "Reference"
] as const;

export const primaryNav = [
  { href: "/", label: "Overview" },
  { href: "/docs", label: "Docs" },
  { href: siteConfig.repoUrl, label: "GitHub", external: true }
];
