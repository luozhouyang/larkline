import { defineConfig } from "astro/config";
import tailwindcss from "@tailwindcss/vite";
import nimbus, {
  defineConfig as defineNimbusConfig,
} from "@cloudflare/nimbus-docs";
import { tableScroll } from "@cloudflare/nimbus-docs/markdown";

const nimbusConfig = defineNimbusConfig({
  // Canonical origin. The /larkline base is set on the Astro config below so
  // GitHub Pages project URLs and in-site links resolve under the same path.
  site: "https://luozhouyang.github.io",
  title: "larkline",
  description:
    "One static binary between Feishu / Lark and your local Claude Code or Codex CLI.",
  locale: "en",
  github: "https://github.com/luozhouyang/larkline",
  socialImageAlt: "larkline documentation",
  sidebar: {
    items: ["index", { autogenerate: { collection: "docs" } }],
  },
  // This is a GitHub Pages project site, so the origin root is not ours;
  // leave /.well-known to the origin and keep llms.txt on the project path.
  agents: { discovery: false },
});

export default defineConfig({
  // https://luozhouyang.github.io/larkline — project pages need an explicit
  // base; Nimbus prefixes in-site links and assets with it.
  site: "https://luozhouyang.github.io",
  base: "/larkline",
  // nimbus:adapter
  output: "static",
  // Tailwind v4 via its Vite plugin (the integration Astro recommends for
  // Tailwind v4 — replaces the PostCSS plugin, which doesn't build under
  // Astro 7's Vite 8 bundler).
  vite: {
    plugins: [tailwindcss()],
  },
  // Hover-prefetch link targets so full-page navigations feel instant without
  // a client-side router.
  prefetch: {
    prefetchAll: true,
    defaultStrategy: "hover",
  },
  integrations: [
    nimbus(nimbusConfig, {
      // Authoring rules are opt-in by design — your repo, your taste. The
      // two below are the load-bearing pair: frontmatter has to validate
      // against the content schema for the page to render properly, and
      // broken internal links are 404s for your readers. Add the others
      // (heading hierarchy, code-block language, style, etc.) when you're
      // ready to enforce them — see `nimbus-docs lint --help`.
      rules: {
        "nimbus/frontmatter-shape": "error",
        "nimbus/internal-link": "error",
      },
      markdown: {
        // Wrap wide tables so they scroll instead of overflowing the page
        // (styled by `.nb-table-scroll` in src/styles/prose.css).
        hastPlugins: [tableScroll()],
        // `## Install {#install}` gives a heading a fixed id. A heading that
        // ends in other `{…}` text, such as `{account_id}` in a .md file, is
        // read as attributes too; remove this to keep it as text.
        features: { headingAttributes: true },
      },
    }),
  ],
});
