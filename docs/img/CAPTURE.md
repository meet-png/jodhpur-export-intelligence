# How to add the README screenshots (2 minutes)

The README has a screenshot slot near the top. A repo with a visual gets
read; a wall of text gets skipped. Add **one** image and you're done.

## The one that matters: the dashboard

1. Open the live app:
   https://jodhpur-export-intelligence-ashjm6at7ctsyq65hwwqxk.streamlit.app/
2. Go to the **"4 · The 12-month forecast"** tab (it has the slider — most
   visually alive) **or** the **"Start here"** tab (shows the 3 headline
   numbers). Either works; the forecast tab is more impressive.
3. Capture it:
   - **Best (animated):** record a ~5-second screen GIF while you drag the
     rig-count slider. Tools: ScreenToGif (Windows), or Xbox Game Bar
     (`Win+G`) → record → convert to GIF. Save as `dashboard.gif`.
   - **Good (static):** full-window screenshot. Save as `dashboard.png`.
4. Put the file in this folder: `docs/img/dashboard.png` (or `.gif`).
5. In `README.md`, find the `SCREENSHOT SLOT` comment near the top and
   **delete the two comment markers** (`<!--` and `-->`) around the
   `![JEIS dashboard](docs/img/dashboard.png)` line. If you used a GIF,
   change `dashboard.png` to `dashboard.gif`.
6. Commit: `git add docs/img/ README.md && git commit -m "docs: add dashboard screenshot" && git push`

Keep the image under ~2 MB and roughly 1200–1600 px wide so it loads fast
and renders crisply on GitHub.

## Optional second image: the architecture

The README has an ASCII architecture diagram that already reads fine. If
you want a polished image version later, make one in draw.io / Excalidraw,
export as `docs/img/architecture.png`, and add `![Architecture](docs/img/architecture.png)`
just above the ASCII block. Not required — the dashboard screenshot is 95%
of the visual payoff.
