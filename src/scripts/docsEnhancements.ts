const COPY_RESET_MS = 1600;

function getCodeLanguage(pre: HTMLPreElement, code: HTMLElement | null) {
  const dataLanguage = pre.dataset.language;
  if (dataLanguage) return dataLanguage;

  const className = code?.className ?? "";
  const match = className.match(/language-([^\s]+)/);
  return match?.[1] ?? "text";
}

function labelForLanguage(language: string) {
  if (!language || language === "text") return "Text";
  if (language === "sh" || language === "shell" || language === "bash") return "Shell";
  if (language === "toml") return "TOML";
  if (language === "json") return "JSON";
  if (language === "yaml" || language === "yml") return "YAML";
  if (language === "http") return "HTTP";
  if (language === "mermaid") return "Mermaid";
  return language.replace(/-/g, " ").replace(/\b\w/g, (char: string) => char.toUpperCase());
}

async function copyText(text: string, button: HTMLButtonElement) {
  const originalLabel = button.textContent ?? "Copy";

  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
    } else {
      const textarea = document.createElement("textarea");
      textarea.value = text;
      textarea.setAttribute("readonly", "");
      textarea.style.position = "fixed";
      textarea.style.opacity = "0";
      document.body.append(textarea);
      textarea.select();
      document.execCommand("copy");
      textarea.remove();
    }

    button.textContent = "Copied";
    button.classList.add("is-copied");
  } catch (error) {
    console.warn("Unable to copy code block", error);
    button.textContent = "Failed";
    button.classList.add("is-failed");
  }

  window.setTimeout(() => {
    button.textContent = originalLabel;
    button.classList.remove("is-copied", "is-failed");
  }, COPY_RESET_MS);
}

function enhanceCodeBlocks() {
  const blocks = document.querySelectorAll<HTMLPreElement>(".docs-copy pre:not([data-docs-enhanced])");
  const mermaidNodes: HTMLElement[] = [];

  blocks.forEach((pre) => {
    const code = pre.querySelector<HTMLElement>("code");
    const source = code?.textContent ?? pre.textContent ?? "";
    const language = getCodeLanguage(pre, code);
    const isMermaid = language === "mermaid";

    pre.dataset.docsEnhanced = "true";

    const wrapper = document.createElement("div");
    wrapper.className = isMermaid ? "docs-codeblock docs-codeblock--diagram" : "docs-codeblock";

    const header = document.createElement("div");
    header.className = "docs-codeblock__header";

    const languageLabel = document.createElement("span");
    languageLabel.className = "docs-codeblock__language";
    languageLabel.textContent = labelForLanguage(language);

    const copyButton = document.createElement("button");
    copyButton.className = "docs-codeblock__copy";
    copyButton.type = "button";
    copyButton.textContent = "Copy";
    copyButton.setAttribute("aria-label", `Copy ${labelForLanguage(language)} code block`);
    copyButton.addEventListener("click", () => copyText(source, copyButton));

    header.append(languageLabel, copyButton);
    pre.before(wrapper);
    wrapper.append(header);

    if (isMermaid) {
      const diagram = document.createElement("div");
      diagram.className = "docs-mermaid mermaid";
      diagram.textContent = source;
      wrapper.append(diagram);
      pre.remove();
      mermaidNodes.push(diagram);
    } else {
      wrapper.append(pre);
    }
  });

  return mermaidNodes;
}

function enhanceTables() {
  const tables = document.querySelectorAll<HTMLTableElement>(".docs-copy table:not([data-docs-enhanced])");

  tables.forEach((table) => {
    table.dataset.docsEnhanced = "true";

    const wrapper = document.createElement("div");
    wrapper.className = "docs-table";
    wrapper.setAttribute("role", "region");
    wrapper.setAttribute("aria-label", "Scrollable documentation table");
    wrapper.tabIndex = 0;

    table.before(wrapper);
    wrapper.append(table);
  });
}

async function renderMermaidDiagrams(nodes: HTMLElement[]) {
  if (nodes.length === 0) return;

  const { default: mermaid } = await import("mermaid");

  mermaid.initialize({
    startOnLoad: false,
    theme: "neutral",
    securityLevel: "strict",
    fontFamily: "Inter, system-ui, sans-serif"
  });

  try {
    await mermaid.run({ nodes });
  } catch (error) {
    console.warn("Unable to render Mermaid diagram", error);
    nodes.forEach((node) => {
      node.classList.add("is-error");
      node.setAttribute("role", "note");
      node.setAttribute("aria-label", "Mermaid diagram source could not be rendered");
    });
  }
}

async function initDocsEnhancements() {
  const mermaidNodes = enhanceCodeBlocks();
  enhanceTables();
  await renderMermaidDiagrams(mermaidNodes);
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initDocsEnhancements, { once: true });
} else {
  initDocsEnhancements();
}
