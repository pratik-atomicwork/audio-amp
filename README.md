# Playlist Player

A single-screen player: one full-bleed random image, and a YouTube playlist
playing behind it through an iframe that is hidden with CSS. Because the
iframe's own controls are hidden along with it, everything is driven by a
custom control bar.

## Run

```bash
npm start          # → http://localhost:5173
```

No dependencies and no build step — `serve.js` is a ~40-line static server on
node's stdlib. It exists because the YouTube IFrame API refuses a `null`
origin, so opening `index.html` over `file://` will not play.

## Controls

| Control | Keys |
| --- | --- |
| Play / pause | `Space`, `K` |
| Seek ±10s | `←` `→`, `J` `L` |
| Previous / next track | `P` / `N` |
| Mute / unmute | `M` |
| Volume ±5 | `↑` `↓` |
| Shuffle | `S` |
| New background image | `R` |

The bar also has a draggable seek slider with elapsed and total time, a volume
slider, and a loop-playlist toggle (on by default). Current title, channel,
and `Track n of m` are shown above the bar.

## Choosing a playlist

Defaults live at the top of the `<script>` in `index.html`, and both are
overridable per-visit without editing the file:

```
?list=<playlistId>          # playlist to load
?v=<videoId>                # track to start on, if it is in that playlist
```

## How the hiding works

The iframe is full-size and on-screen, hidden with `opacity: 0` and
`pointer-events: none`, and painted over by the image layer. It is
deliberately *not* hidden with `display: none` or by positioning it
off-screen — browsers throttle or pause video treated that way, which stops
playback.

Two consequences of hiding a YouTube embed are worth knowing:

- **Playback needs a gesture.** Autoplay with sound is blocked, so the page
  opens with a start button. That first tap is what unmutes and plays.
- **Track titles come one at a time.** `getVideoData()` reports only the
  current video; `getPlaylist()` returns bare video IDs. Rendering the whole
  playlist as a clickable tracklist would need the YouTube Data API and an
  API key.

Background images come from [picsum.photos](https://picsum.photos) with a
fresh seed per load, sized to the viewport and device pixel ratio, falling
back to a generated gradient if the fetch fails. Videos that block embedding
surface a toast and are skipped automatically.
