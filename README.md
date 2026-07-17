# DW Spectrum (IPVMS) — Home Assistant Integration

This integration brings your **DW Spectrum / Digital Watchdog** system into Home Assistant so you can **view cameras**, **control recording behavior**, and **monitor system capacity** from one place.

---

## What you get

### Camera access in Home Assistant
- Your DW Spectrum cameras show up as **camera entities** in Home Assistant.
- Designed to work cleanly inside HA dashboards and automations.
- Includes an HA-side option to **block live streaming** per camera (see below).

### Live stream privacy control (per camera)
- **"Block Live Stream in HA"** switch for each camera.
- When enabled, Home Assistant will **not provide a stream URL** for that camera — ideal for privacy, bandwidth control, or preventing accidental viewing from dashboards.
- This is **Home Assistant–side only** and does not change your DW Spectrum server settings.

### Recording controls (per camera)
- **Recording Disabled** switch per camera to quickly disable recording.
- Recording mode controls per camera:
  - **Always Record**
  - **Motion Only**
  - **Motion + Low Res**
- These modes are exposed as simple toggles so you can change recording behavior quickly or build automations (e.g. different recording rules for business hours vs. nights).

### Motion detection
- Motion sensors work automatically **without any configuration** — no callback URL needed.
- The integration polls the DW Spectrum API every 5 seconds; motion is detected within 3–15 seconds of starting and clears 20–30 seconds after it stops.
- For **instant motion detection** (under 1 second), enter a Home Assistant URL that the DW server can reach (e.g. `http://192.168.1.50:8123` for local use or `https://ha.example.com` for remote access). The integration will create DW event rules that push motion start/stop to HA in real time.

### System status & capacity sensors
- **Camera count**
- **Licenses total**
- **Licenses used**
- **Licenses available**

These sensors make it easy to monitor capacity at a glance and automate alerts when you're running low.

### User management controls
- Enable or disable **DW Spectrum user accounts** directly from Home Assistant.
- Useful for temporary staff, after-hours access control, or security workflows.

### Remembers your choices
- Stream blocking and recording mode selections are **saved and restored**, so your settings persist through HA restarts.

---

## Typical use cases
- Add cameras to dashboards without giving everyone live stream access (use **Block Live Stream in HA**).
- Automate recording rules based on time, occupancy, or alarm status.
- Monitor license usage and get alerted when you're nearing your limit.
- Quickly disable recording for specific cameras during maintenance.
- Lock or unlock DW Spectrum user accounts from HA automations.

## Home Assistant Lovelace Card

We created a custom Lovelace frontend card for your Home Assistant dashboard.

[Click here to download it](https://github.com/igraph100/dw_spectrum_playback_card)
