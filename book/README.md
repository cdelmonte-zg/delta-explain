# Documentation site (mdBook)

The published site: <https://cdelmonte-zg.github.io/delta-explain/>. It is built
from `book/src/` with [mdBook](https://rust-lang.github.io/mdBook/) and deployed
by `.github/workflows/pages.yml` on every push to `main` that touches the docs.

## Single source of truth

The reference chapters and the project pages are **symlinks** into the
canonical files, so they are never duplicated:

- `src/reference/{semantics,json-schema,validation}.md` -> `docs/`
- `src/project/roadmap.md` -> `VISION.md`, `src/project/changelog.md` -> `CHANGELOG.md`

The guides, getting-started, and concepts pages are written for the site (the
navigable front door the scattered markdown lacked).

## Build locally

```bash
cargo install mdbook          # once
cd book && mdbook serve        # live-reload at http://localhost:3000
# or: mdbook build             # renders to book/book/ (gitignored)
```
