# DW Spectrum (IPVMS) — Home Assistant Integration

This integration brings your **DW Spectrum / Digital Watchdog** system into Home Assistant so you can **view cameras**, **control recording behavior**, and **monitor system capacity** from one place.

## What you get

### Camera access in Home Assistant

* Your DW Spectrum cameras show up as **camera entities** in Home Assistant.
* Designed to work cleanly inside HA dashboards and automations.
* Includes an HA-side option to **block live streaming** per camera (see below).

### Live stream privacy control (per camera)

* **“Block Live Stream in HA”** switch for each camera.
* When enabled:

  * Home Assistant will **not provide a stream URL** for that camera.
  * This is ideal for privacy, bandwidth control, or preventing accidental viewing from dashboards.

* Important: This is **Home Assistant–side only** (it does not change your DW Spectrum server settings by itself).

### Recording controls (per camera)

* **Recording Disabled** switch per camera to quickly disable recording behavior.
* Recording mode controls per camera:

  * **Always Record**
  * **Motion Only**
  * **Motion + Low Res**

* These modes are exposed as simple toggles so you can:

  * Change recording behavior quickly
  * Build automations (e.g., different recording rules for business hours vs nights)

### System status \& capacity sensors

* **Camera count**
* **Licenses total**
* **Licenses used**
* **Licenses available**

These sensors make it easy to monitor capacity and licensing at a glance (and automate alerts if you’re running low).

### Remembers your choices

* Stream blocking and recording mode selections are **saved and restored**, so your settings persist through restarts.

## Typical use cases

* Add cameras to dashboards without giving everyone live stream access (use **Block Live Stream in HA**).
* Automate recording rules based on time, occupancy, or alarm status.
* Monitor license usage so you know when you’re nearing your limit.
* Quickly disable recording for specific cameras during maintenance.
  



### User management controls

* User-level controls to **enable or disable DW Spectrum users** from Home Assistant.
* Useful for quickly turning access on/off (e.g., temporary staff, after-hours access, security workflows).



