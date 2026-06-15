# Mock Workday Tenant

A local **Workday integration playground** you can run on your own machine. No live tenant, no license, no waiting on IT. Just open it up and practice.

---

## Why this exists

Workday integration work is hard to practice when you don't have a tenant sitting in front of you. Real tenants cost money, need provisioning, and you can't break things freely.

This project fakes the parts that matter. It mimics the screens, flows, and behavior of a real Workday tenant closely enough that you can **rehearse the things interviewers actually ask about**: EIBs, Studio integrations, error handling, spreadsheet templates, and so on.

If something here behaves differently from real Workday, that's a bug worth fixing. Accuracy is the whole point.

---

## What's inside

**Inbound EIB**
- Launch / Schedule Integration flow with an Integration Criteria grid
- Typed file attachment upload
- **Load Error Limit** (hits the threshold, status goes to `ABORTED`)
- Validate Only load mode
- Add Errors to Attachment (downloads an errors CSV)
- Generate Spreadsheet Template (real SpreadsheetML `.xml` with an Overview sheet plus banded operation sheets)
- A My Reports page to download what you generated

**Workday Studio canvas**
- Eclipse-style layout: horizontal left-to-right flow with arrow connectors
- Mediation containers drawn as titled boxes with color-coded step strips
  - MVEL = orange, XSLT = purple, LOG = blue, STO = gold
- SendError lane strips

**Studio engine**
- Typed Launch Parameters (`Name:type=value`)
- MVEL helpers: `lp.getdate()`, `lp.gettext()`, `lp.getreferenceData()` with a seeded Organization Reference ID map (Finance, Engineering, HR, Legal, Sales)
- Subscript assignment in MVEL (`props['key'] = value`)
- Full Error Handlers palette with catch-and-divert logic

---

## Requirements

- **Python 3** (any recent 3.x)
- **Flask**

Install Flask if you don't have it:

```bash
pip install flask
```

---

## Quick start

1. Open a terminal in the project folder
2. Run the server:

```bash
python workday_ui.py
```

3. Open your browser to:

```
http://localhost:8443/home
```

That's it. Click around the same way you would in a real tenant.

---

## Updating to a new build

When a fresh `mock-workday.zip` is ready, follow these steps in order:

1. **Stop the server** (Ctrl+C in the terminal)
2. **Download** the new `mock-workday.zip` to your Downloads folder
3. **Unzip over the project** (PowerShell):

```powershell
powershell -command "Expand-Archive -Force '%USERPROFILE%\Downloads\mock-workday.zip' 'D:\Manju\Workday\mock-workday'"
```

4. **Restart** the server, then **hard refresh** the browser with `Ctrl+F5`

> Always hard refresh after an update. A normal refresh can serve stale cached pages and make a fixed bug look like it's still there.

---

## Known issues

- **Studio `workday-out-rest` Extra Path prop**: the value must end exactly with `')}` and **not** `')})`. The extra trailing `)` causes a "report not found" error.

---

## Notes

This is a practice tool, not a Workday replacement. It exists to build muscle memory and confidence before the real thing. When the mock and real Workday disagree, trust real Workday (and fix the mock).
