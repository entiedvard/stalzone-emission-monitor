# Astra Emission Monitor

Rust Astra plugin for monitoring STALZONE emissions through the official API.

It provides chat tools for current and last emission status, polls for new
emissions, sends deduplicated Astra chat notifications, and stores only the
last observed emission state in the plugin data directory.

## Development

Language: rust

```bash
astra-plugin dev .
```

## Build

```bash
astra-plugin build
```

This produces a `.astraplugin` file for distribution.

## How this file is used

When this plugin is listed, Astra's store shows this README on the plugin's
page — so what you write here is what someone reads while deciding whether to
install it. `icon.svg` next to `plugin.toml` becomes the store card's picture;
replace it with your own.

Screenshots work, and are the reason to add any:

```markdown
![The command editor, mid-roll](docs/screenshot.png)
```

Commit the images to this repository and link them with a **relative path**.
The registry rewrites those to point at the exact commit your release was built
from, so a picture cannot change after somebody approved the listing. Images
hosted anywhere but GitHub are dropped rather than rendered — a remote image is
a request from the user's machine, made before they have installed anything.

Raw HTML is stripped, so use markdown for layout.
