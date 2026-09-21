# -*- coding: utf-8 -*-
"""
orchestrate.py  --  Workday Orchestrate clone for the mock-workday tenant.

Replicates the real developer.workday.com Orchestration Builder plus the
end-to-end lifecycle:

  Developer Site  ->  Build an Integration App  ->  add Orchestration
  ->  Orchestration Builder (Trigger -> Send RaaS -> Store Document -> End)
  ->  Validate  ->  Build  ->  Deploy to tenant
  ->  Tenant: View Integration System (Orchestrate template)
  ->  Launch / Schedule Integration (Run Now)
  ->  View Background Process  ->  Output Files  ->  RAAS DATA (.txt)

INSTALL
-------
1. Drop this file next to workday_ui.py.
2. In workday_ui.py, inside the `if __name__ == "__main__":` block, add ONE line
   next to your other `import core_connector` / `import security` lines:

       import orchestrate     # noqa: F401  (registers Workday Orchestrate)

3. Restart, Ctrl+F5, then open:  http://localhost:8443/orchestrate
   (or type "Orchestrate" in the tenant search bar)

Self-contained: only needs Flask + the `app` object from workday_ui.
"""

import os
import re
import ast
import json
import time
import uuid
import random
import string
import datetime
import urllib.request
import urllib.error
import urllib.parse

from flask import request, Response

import workday_ui as wd            # the running app aliases itself in __main__
app = wd.app

# show up in the tenant task search bar
try:
    if not any(t.get("url") == "/orchestrate" for t in wd.TASKS):
        wd.TASKS.append({"name": "Workday Orchestrate", "url": "/orchestrate"})
except Exception:
    pass

STORE = "orchestrate_store.json"

# ===========================================================================
# Sample RaaS dataset (what "Send Workday RaaS Request" returns)
# ===========================================================================
RAAS_ROWS = [
    {"Employee_ID": "21001", "Worker": "Logan McNeil",  "Manager": "Joy Banks",
     "Org": "Engineering", "Email": "lmcneil@corp.com"},
    {"Employee_ID": "21002", "Worker": "Priya Menon",   "Manager": "Logan McNeil",
     "Org": "Finance",     "Email": "pmenon@corp.com"},
    {"Employee_ID": "21003", "Worker": "Diego Reyes",   "Manager": "Logan McNeil",
     "Org": "Sales",       "Email": "dreyes@corp.com"},
    {"Employee_ID": "21004", "Worker": "Mei Lin",       "Manager": "Logan McNeil",
     "Org": "Engineering", "Email": "mlin@corp.com"},
    {"Employee_ID": "21005", "Worker": "Omar Haddad",   "Manager": "Logan McNeil",
     "Org": "HR",          "Email": "ohaddad@corp.com"},
]

TENANTS = ["wday_wcpdev40", "wday_wcpdev41", "wday_wcpdev42", "wday_wcpdev43",
           "wday_wcpdev47", "wday_wcpdev48", "wday_wcpdev49", "wday_wcpdev5"]


def _ref_suffix():
    return "".join(random.choice(string.ascii_lowercase) for _ in range(6))


def seed():
    return {
        "apps": {
            "mw_dpdd": {
                "id": "mw_dpdd",
                "name": "MW DPDD 2026 June",
                "refId": "mw_dpdd",
                "appId": "85c8561686022e6e95aabc21eb79c477",
                "description": "Aggregate deep dive: Custom strategy, Initial/Next Value, Conditions, pill vs expression mode",
                "created": "06/18/2026",
                "createdBy": "Marek Wichtowski",
                "orchestrations": {
                    "POC": {
                        "name": "POC",
                        "type": "Synchronous",
                        "startType": "synchronous",
                        "lastBuild": None,
                        "steps": [
                            {
                                "id": "s_poc_ctt",
                                "type": "create-text-template",
                                "ref": "CreateTextTemplate",
                                "props": {
                                    "contentType": "application/json",
                                    "message": '{\n  "items": ["UUID", "Sarah", "James", "UUID", "Diana"]\n}',
                                },
                            },
                            {
                                "id": "s_poc_loop",
                                "type": "loop",
                                "ref": "Loop",
                                "props": {
                                    "dataType": "AutoType Iterator",
                                    "dataSetRef": "CreateTextTemplate.message",
                                    "dataSetPath": "$.items[*]",
                                    "filterPath": "",
                                    "sortBy": [],
                                    "locale": "",
                                },
                                "body": [
                                    {
                                        "id": "s_poc_log",
                                        "type": "log",
                                        "ref": "Log",
                                        "props": {"messageRef": "Loop.item", "messageFn": "toString",
                                                  "condition": "true"},
                                    }
                                ],
                                "aggregation": {
                                    "ref": "Aggregate",
                                    "failWhenNoInputs": False,
                                    "earlyStop": None,
                                    "errorHandler": False,
                                    "deleted": False,
                                    "outputs": [
                                        {
                                            "name": "EMP",
                                            "strategy": "Custom",
                                            "outputType": "JSON",
                                            # {} .asJSON() .addStringValue(Loop.item, "TEST")
                                            "initialValue": [
                                                {"t": "json"},
                                                {"t": "fn", "v": "asJSON"},
                                                {"t": "fn", "v": "addStringValue"},
                                                {"t": "ref", "v": "Loop.item"},
                                                {"t": "str", "v": "TEST"},
                                            ],
                                            # Aggregate.EMP.addStringValue(Loop.item,
                                            #   ((Loop.item == "UUID") randomUUID() else "TEST"+Loop.itemNumber))
                                            "nextValue": [
                                                {"t": "ref", "v": "Aggregate.EMP"},
                                                {"t": "fn", "v": "addStringValue"},
                                                {"t": "ref", "v": "Loop.item"},
                                                {"t": "cond", "mode": "all",
                                                 "rows": [{"left": [{"t": "ref", "v": "Loop.item"}],
                                                           "op": "Is equal to",
                                                           "right": [{"t": "str", "v": "UUID"}]}],
                                                 "ifTrue": [{"t": "fn", "v": "randomUUID"}],
                                                 "ifFalse": [{"t": "str", "v": "TEST"},
                                                             {"t": "ref", "v": "Loop.itemNumber"}]},
                                            ],
                                            "jsonFragment": [],
                                            "condition": [],
                                        }
                                    ],
                                },
                            },
                        ],
                    },
                },
            },
            "raas_nkzjqw": {
                "id": "raas_nkzjqw",
                "name": "raas",
                "refId": "raas_nkzjqw",
                "appId": "a39edc955766c3c20a4bb48edeafce06",
                "description": "--",
                "created": "04/24/2026",
                "createdBy": "Tony Gilfillan",
                "orchestrations": {
                    "raas": {
                        "name": "raas",
                        "type": "Workday Integration System",
                        "startType": "integration",
                        "lastBuild": None,
                        "steps": [
                            {
                                "id": "s_raas",
                                "type": "send-workday-raas-request",
                                "ref": "SendWorkdayRaaSRequest",
                                "props": {
                                    "method": "GET",
                                    "urlPrefix": "http://localhost:8443/task/view-report?name=",
                                    "path": "CRT_INT01_Raas",
                                    "auth": "Default Workday API Credential",
                                    "contentType": "Any",
                                },
                            },
                            {
                                "id": "s_store",
                                "type": "store-document",
                                "ref": "StoreDocument",
                                "props": {
                                    "documentToStore": [{"t": "ref", "v": "SendWorkdayRaaSRequest.response"}],
                                    "documentTitle": [{"t": "string", "v": "RAAS DATA"}],
                                    "description": [],
                                    "collection": [],
                                    "expiresIn": "7",
                                    "expiresUnit": "Days",
                                    "attachToEvent": "true",
                                    "deliver": "false",
                                },
                            },
                        ],
                    },
                    "Looping": {
                        "name": "Looping",
                        "type": "Synchronous",
                        "startType": "synchronous",
                        "lastBuild": None,
                        "steps": [
                            {
                                "id": "s_ctt",
                                "type": "create-text-template",
                                "ref": "CreateTextTemplate",
                                "props": {
                                    "contentType": "application/json",
                                    "message": ('{\n  "company": {\n    "employees": [\n'
                                                '      {"id":101,"name":"Sarah Connor","role":"Manager","department":"Operations","active":true},\n'
                                                '      {"id":102,"name":"James Howlett","role":"Security Specialist","department":"Security","active":true},\n'
                                                '      {"id":103,"name":"Diana Prince","role":"Legal Consultant","department":"Legal","active":false},\n'
                                                '      {"id":104,"name":"Tony Stark","role":"Lead Engineer","department":"R&D","active":true}\n'
                                                '    ]\n  }\n}'),
                                },
                            },
                            {
                                "id": "s_loop",
                                "type": "loop",
                                "ref": "Loop",
                                "props": {
                                    "dataType": "AutoType Iterator",
                                    "dataSetRef": "CreateTextTemplate.message",
                                    "dataSetPath": "$.company.employees[*]",
                                    "filterPath": "$.active",
                                    "sortBy": [{"path": "$.role", "dir": "asc"}],
                                    "locale": "",
                                },
                                "body": [
                                    {
                                        "id": "s_log",
                                        "type": "log",
                                        "ref": "Log",
                                        "props": {"messageRef": "Loop.item", "messageFn": "toString",
                                                  "condition": "true"},
                                    }
                                ],
                                "aggregation": {
                                    "ref": "Aggregate",
                                    "outputs": [{"name": "ActiveUsers", "strategy": "JSON"}],
                                    "failWhenNoInputs": False,
                                    "earlyStop": None,
                                    "errorHandler": False,
                                    "deleted": False,
                                },
                            },
                        ],
                    },
                },
            },
            "stock_app": {
                "id": "stock_app",
                "name": "StockNotifications",
                "refId": "stock_app",
                "appId": "6f4026e444f4f3f37ac1d53d4a63a298",
                "description": "Retrieve stock prices from an external API",
                "created": "06/27/2026",
                "createdBy": "Tony Gilfillan",
                "orchestrations": {
                    "StockRetrieval": {
                        "name": "StockRetrieval",
                        "type": "Synchronous",
                        "startType": "synchronous",
                        "lastBuild": None,
                        "steps": [
                            {
                                "id": "s_http",
                                "type": "send-http-request",
                                "ref": "SendHTTPRequest",
                                "props": {
                                    "method": "GET",
                                    "url": "http://localhost:8443/orchestrate/mock-api/stock",
                                    "auth": "No Auth",
                                    "advancedMode": False,
                                    "queryParams": [
                                        {"key": "ticker", "value": "AAPL"},
                                        {"key": "apiKey", "value": "demo"},
                                    ],
                                },
                            },
                            {
                                "id": "s_cv",
                                "type": "create-values",
                                "ref": "CreateValues",
                                "props": {
                                    "values": [
                                        {"name": "Ticker", "sourceRef": "SendHTTPRequest.response",
                                         "jsonPath": "$.results[0].T"},
                                        {"name": "OpenPrice", "sourceRef": "SendHTTPRequest.response",
                                         "jsonPath": "$.results[0].o"},
                                        {"name": "ClosePrice", "sourceRef": "SendHTTPRequest.response",
                                         "jsonPath": "$.results[0].c"},
                                    ],
                                },
                            },
                            {
                                "id": "s_log2",
                                "type": "log",
                                "ref": "Log",
                                "props": {"messageRef": "CreateValues.OpenPrice", "condition": "true"},
                            },
                        ],
                    },
                },
            },
            "hackathon_app": {
                "id": "hackathon_app",
                "name": "HackathonTickets",
                "refId": "hackathontickets_svfbfp",
                "appId": "5d5ed13bc9cfee28a9ec1bc9369ebeef",
                "description": "Hackathon ticket registration app",
                "created": "02/24/2026",
                "createdBy": "Tony Gilfillan",
                "orchestrations": {
                    "AddUserToSlack": {
                        "name": "AddUserToSlack",
                        "type": "Synchronous Orchestration",
                        "startType": "synchronous",
                        "lastBuild": None,
                        "steps": [
                            {"id": "s_log", "type": "log", "ref": "Log",
                             "props": {"message": "USER ADDED - ORCHESTRATION HIT", "condition": "true"}},
                        ],
                    },
                    "AddUserBPTrigger": {
                        "name": "AddUserBPTrigger",
                        "type": "Workday Business Process",
                        "startType": "business-process",
                        "lastBuild": None,
                        "steps": [
                            {"id": "s_log", "type": "log", "ref": "Log",
                             "props": {"message": "ADD USER _ BP TRIGGER", "condition": "true"}},
                        ],
                    },
                },
            },
            "suporg_app": {
                "id": "suporg_app",
                "name": "SupOrgManagement",
                "refId": "suporgmanagement_svfbfp",
                "appId": "7a1c0e9b2d4f46a8b3c5d7e9f1a2b3c4",
                "description": "Create Supervisory Organizations via Add_Update_Organization",
                "created": "06/29/2026",
                "createdBy": "Vasanth",
                "orchestrations": {
                    "CreateSupOrg": {
                        "name": "CreateSupOrg",
                        "type": "Synchronous Orchestration",
                        "startType": "synchronous",
                        "lastBuild": None,
                        "steps": [
                            # 1+2  receive input (Input.*) + validate required fields
                            {"id": "s_val", "type": "create-values", "ref": "ValidateInput",
                             "props": {"values": [
                                 {"name": "code", "sourceRef": "Input", "jsonPath": "$.SupOrgCode"},
                                 {"name": "name", "sourceRef": "Input", "jsonPath": "$.SupOrgName"},
                             ]}},
                            # 3  check if Sup Org code already exists
                            {"id": "s_chk", "type": "send-http-request", "ref": "CheckExists",
                             "props": {"method": "GET", "auth": "No Auth",
                                       "url": "http://localhost:8443/tenant/soap/human_resources/exists",
                                       "queryParams": [{"key": "code", "value": "{Input.SupOrgCode}"}]}},
                            # 4  read the exists flag
                            {"id": "s_exists", "type": "create-values", "ref": "Existence",
                             "props": {"values": [
                                 {"name": "exists", "sourceRef": "CheckExists.response", "jsonPath": "$.exists"},
                             ]}},
                            # 5  build the SOAP request payload (Add_Update_Organization)
                            {"id": "s_soap", "type": "create-text-template", "ref": "BuildSoapPayload",
                             "props": {"contentType": "text/xml", "message": (
                                 '<?xml version="1.0" encoding="UTF-8"?>\n'
                                 '<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/" '
                                 'xmlns:wd="urn:com.workday/bsvc">\n'
                                 '  <env:Body>\n'
                                 '    <wd:Add_Update_Organization_Request wd:version="v43.0">\n'
                                 '      <wd:Organization_Data>\n'
                                 '        <wd:Organization_Code>{Input.SupOrgCode}</wd:Organization_Code>\n'
                                 '        <wd:Organization_Name>{Input.SupOrgName}</wd:Organization_Name>\n'
                                 '        <wd:Organization_Type_Reference>Supervisory</wd:Organization_Type_Reference>\n'
                                 '        <wd:Organization_Subtype_Reference>{Input.OrgSubtype}</wd:Organization_Subtype_Reference>\n'
                                 '        <wd:Availability_Date>{Input.AvailabilityDate}</wd:Availability_Date>\n'
                                 '        <wd:Include_Manager_in_Name>true</wd:Include_Manager_in_Name>\n'
                                 '        <wd:Superior_Organization_Reference>{Input.SuperiorOrg}</wd:Superior_Organization_Reference>\n'
                                 '        <wd:Staffing_Model>{Input.StaffingModel}</wd:Staffing_Model>\n'
                                 '        <wd:Location_Reference>{Input.PrimaryLocation}</wd:Location_Reference>\n'
                                 '        <wd:Manager_Reference>{Input.Manager}</wd:Manager_Reference>\n'
                                 '      </wd:Organization_Data>\n'
                                 '    </wd:Add_Update_Organization_Request>\n'
                                 '  </env:Body>\n</env:Envelope>')}},
                            # 6+7  call Human_Resources / Add_Update_Organization, capture response
                            {"id": "s_call", "type": "send-http-request", "ref": "CallHumanResources",
                             "props": {"method": "POST", "auth": "Default Workday API Credential",
                                       "contentType": "text/xml",
                                       "url": "http://localhost:8443/tenant/soap/human_resources",
                                       "bodyRef": "BuildSoapPayload.message"}},
                            {"id": "s_cap", "type": "create-values", "ref": "Result",
                             "props": {"values": [
                                 {"name": "orgRef", "sourceRef": "CallHumanResources.response",
                                  "jsonPath": "$.Organization_Reference"},
                                 {"name": "status", "sourceRef": "CallHumanResources.response", "jsonPath": "$.status"},
                                 {"name": "message", "sourceRef": "CallHumanResources.response", "jsonPath": "$.message"},
                             ]}},
                            # 8  return success/error message
                            {"id": "s_log", "type": "log", "ref": "Log",
                             "props": {"messageRef": "Result.message", "condition": "true"}},
                        ],
                    },
                },
            },
        },
        "builds": [],
        "deployments": [],
        "integration_systems": {},
        "bg_processes": {},
        "output_files": {},
        "counters": {"build": 2, "bp": 0, "of": 0, "is": 0},
    }


def _norm_out(o):
    """Ensure an aggregate output has every field the full editor expects."""
    o.setdefault("strategy", "JSON")
    o.setdefault("outputType", "JSON")
    for k in ("initialValue", "nextValue", "jsonFragment", "condition"):
        if not isinstance(o.get(k), list):
            o[k] = []
    # Formatted CSV strategy (Orchestrate "FormattedCsvFoldValue")
    o.setdefault("csvColumns", "")
    o.setdefault("csvDelimiter", ",")
    o.setdefault("csvHeader", False)
    return o


def _migrate(d):
    """Merge newly-seeded apps and normalize old aggregation outputs.
    Runs on every load so an existing orchestrate_store.json never has to be
    deleted to pick up the Aggregate editor upgrade."""
    changed = False
    apps = d.setdefault("apps", {})
    if "mw_dpdd" not in apps:
        apps["mw_dpdd"] = seed()["apps"]["mw_dpdd"]
        changed = True

    def walk(steps):
        nonlocal changed
        for st in steps or []:
            ag = st.get("aggregation")
            if isinstance(ag, dict):
                for o in ag.get("outputs") or []:
                    before = json.dumps(o, sort_keys=True)
                    _norm_out(o)
                    if json.dumps(o, sort_keys=True) != before:
                        changed = True
            walk(st.get("body"))

    for a in apps.values():
        for orch in (a.get("orchestrations") or {}).values():
            walk(orch.get("steps"))
    return changed


def load():
    if not os.path.exists(STORE):
        save(seed())
    try:
        with open(STORE, "r", encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
        d = seed()
    try:
        if _migrate(d):
            save(d)
    except Exception:
        pass
    return d


def save(data):
    with open(STORE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def esc(t):
    return (str(t) if t is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ===========================================================================
# Self-contained mock stock API
# Gives "Send HTTP Request" a live JSON endpoint to hit with no external key.
# Shape mirrors Polygon.io so JSON paths match real practice
# ($.results[0].o = open, .c = close, .h/.l = high/low, .v = volume).
# Swap the orchestration URL to a real API anytime; the engine GETs either way.
# ===========================================================================
_STOCK_SEED = {
    "AAPL": {"o": 189.33, "h": 192.10, "l": 188.50, "c": 191.24, "v": 54213400},
    "MSFT": {"o": 421.05, "h": 425.66, "l": 419.80, "c": 424.12, "v": 19872300},
    "TSLA": {"o": 245.10, "h": 251.44, "l": 243.02, "c": 249.88, "v": 88345100},
    "NVDA": {"o": 121.40, "h": 124.95, "l": 120.10, "c": 123.77, "v": 301244500},
}


@app.route("/orchestrate/mock-api/stock")
def mock_stock_api():
    ticker = (request.args.get("ticker") or "AAPL").upper()
    base = _STOCK_SEED.get(ticker, _STOCK_SEED["AAPL"])
    jitter = lambda x: round(x * (1 + random.uniform(-0.01, 0.01)), 2)  # noqa: E731
    payload = {
        "status": "OK",
        "request_id": _ref_suffix(),
        "ticker": ticker,
        "results": [{
            "T": ticker,
            "o": jitter(base["o"]),
            "h": jitter(base["h"]),
            "l": jitter(base["l"]),
            "c": jitter(base["c"]),
            "v": base["v"],
            "t": int(time.time() * 1000),
        }],
    }
    return Response(json.dumps(payload), mimetype="application/json")


# ===========================================================================
# Engine (executes an orchestration; used by Run and by tenant Launch)
# ===========================================================================
def resolve_tokens(tokens, ctx):
    """Concatenate a token list ([{t:string|ref, v}]) into a value/string."""
    if not tokens:
        return ""
    parts = []
    for tok in tokens:
        if tok.get("t") == "ref":
            parts.append(get_path(ctx, tok.get("v", "")))
        else:
            parts.append(tok.get("v", ""))
    if len(parts) == 1:
        return parts[0]
    return "".join(str(p) for p in parts)


def get_path(ctx, path):
    cur = ctx
    for key in str(path).split("."):
        if isinstance(cur, dict):
            cur = cur.get(key)
        else:
            return None
    return cur


def fetch_report(full_url):
    """GET the configured report URL and parse whatever comes back.
    Works against the user's own tenant (e.g. http://localhost:8443/task/view-report?name=...)
    or any external RaaS endpoint. Never hardcodes data."""
    if not full_url or not str(full_url).lower().startswith(("http://", "https://")):
        return {"response": {"error": "No valid URL configured"}, "responseStatusCode": 0,
                "responseHeaders": {}, "error": "No valid URL configured"}
    try:
        req = urllib.request.Request(full_url, headers={"User-Agent": "WorkdayOrchestrate/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read().decode("utf-8", "replace")
            ct = r.headers.get("Content-Type", "")
            return {"response": parse_report(body), "responseStatusCode": getattr(r, "status", 200),
                    "responseHeaders": {"Content-Type": ct}}
    except urllib.error.HTTPError as e:
        return {"response": {"error": "HTTP %s" % e.code}, "responseStatusCode": e.code,
                "responseHeaders": {}, "error": "HTTP %s" % e.code}
    except Exception as e:
        return {"response": {"error": str(e)}, "responseStatusCode": 0,
                "responseHeaders": {}, "error": str(e)}


def subst(text, ctx):
    """Replace {Some.Path} tokens in a string with values pulled from ctx.
    Tightly scoped to {Word.Word} so JSON/XML braces are never touched."""
    if not isinstance(text, str):
        return text

    def _r(m):
        v = get_path(ctx, m.group(1))
        return str(v) if v is not None else m.group(0)
    return re.sub(r"\{([A-Za-z0-9_.]+)\}", _r, text)


def http_send(url, method, body, content_type="application/json"):
    """POST/PUT/PATCH/DELETE with a request body (SOAP or JSON)."""
    if not url or not str(url).lower().startswith(("http://", "https://")):
        return {"response": {"error": "No valid URL configured"}, "responseStatusCode": 0,
                "responseHeaders": {}, "error": "No valid URL configured"}
    try:
        data = (body or "").encode("utf-8")
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"User-Agent": "WorkdayOrchestrate/1.0",
                                              "Content-Type": content_type})
        with urllib.request.urlopen(req, timeout=10) as r:
            txt = r.read().decode("utf-8", "replace")
            ct = r.headers.get("Content-Type", "")
            return {"response": parse_report(txt), "responseStatusCode": getattr(r, "status", 200),
                    "responseHeaders": {"Content-Type": ct}}
    except urllib.error.HTTPError as e:
        try:
            txt = e.read().decode("utf-8", "replace")
        except Exception:
            txt = ""
        return {"response": parse_report(txt) if txt else {"error": "HTTP %s" % e.code},
                "responseStatusCode": e.code, "responseHeaders": {}, "error": "HTTP %s" % e.code}
    except Exception as e:
        return {"response": {"error": str(e)}, "responseStatusCode": 0,
                "responseHeaders": {}, "error": str(e)}


def parse_report(body):
    """Turn a report response into structured rows when possible."""
    b = (body or "").strip()
    if not b:
        return []
    if b[:1] in "[{":
        try:
            return json.loads(b)
        except Exception:
            pass
    if "<table" in b.lower():
        rows = extract_html_table(b)
        if rows:
            return rows
    if "<" not in b[:300] and ("\n" in b) and ("," in b or "\t" in b):
        return parse_delimited(b)
    return b  # raw text


def extract_html_table(html):
    m = re.search(r"<table.*?</table>", html, re.I | re.S)
    if not m:
        return []
    trs = re.findall(r"<tr.*?</tr>", m.group(0), re.I | re.S)
    headers, rows = [], []
    for tr in trs:
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", tr, re.I | re.S)
        cells = [re.sub(r"<[^>]+>", "", c).replace("&nbsp;", " ").strip() for c in cells]
        if not cells:
            continue
        if not headers:
            headers = cells
        else:
            rows.append(dict(zip(headers, cells)))
    return rows


def parse_delimited(text):
    import csv
    import io
    delim = "\t" if "\t" in text.splitlines()[0] else ","
    return list(csv.DictReader(io.StringIO(text), delimiter=delim))


def jsonpath(data, path):
    """Minimal JSONPath: $.a.b[*].c , $.x , [n]."""
    if data is None:
        return None
    p = (path or "").strip()
    if p.startswith("$"):
        p = p[1:]
    cur = [data]; multi = False
    for key, idx in re.findall(r"\.([A-Za-z0-9_]+)|\[(\*|\d+)\]", p):
        nxt = []
        for node in cur:
            if key and isinstance(node, dict):
                nxt.append(node.get(key))
            elif idx == "*" and isinstance(node, list):
                nxt.extend(node); multi = True
            elif idx.isdigit() and isinstance(node, list) and int(idx) < len(node):
                nxt.append(node[int(idx)])
        cur = nxt
    if multi:
        return cur
    return cur[0] if len(cur) == 1 else cur


def truthy(v):
    if isinstance(v, str):
        return v.strip().lower() not in ("", "false", "0", "none", "null")
    return bool(v)


# ===========================================================================
# Aggregate expression evaluator (pill mode + expression mode + conditions)
# Mirrors the Orchestrate Aggregate editor: Initial Value / Next Value /
# JSON Fragment / Condition fields built from pills, or raw text expressions.
# ===========================================================================
def _cmp(op, l, r):
    ls = "" if l is None else str(l)
    rs = "" if r is None else str(r)
    if op == "Is equal to":
        return ls == rs
    if op == "Is not equal to":
        return ls != rs
    if op == "Contains":
        return rs in ls
    if op == "Does not contain":
        return rs not in ls
    if op == "Starts with":
        return ls.startswith(rs)
    if op == "Greater than":
        try:
            return float(l) > float(r)
        except Exception:
            return ls > rs
    if op == "Less than":
        try:
            return float(l) < float(r)
        except Exception:
            return ls < rs
    if op == "Is empty":
        return ls == ""
    if op == "Is not empty":
        return ls != ""
    return False


def eval_cond_token(tok, ctx):
    """Evaluate a conditional-value pill created by 'Add Conditions for Value'."""
    rows = tok.get("rows") or []
    results = [_cmp(r.get("op", "Is equal to"),
                    agg_eval(r.get("left") or [], ctx),
                    agg_eval(r.get("right") or [], ctx)) for r in rows]
    ok = all(results) if tok.get("mode", "all") == "all" else any(results)
    branch = tok.get("ifTrue") if ok else tok.get("ifFalse")
    if not branch:
        raise ValueError("Expression contains a placeholder")
    return agg_eval(branch, ctx)


def agg_eval(tokens, ctx):
    """Evaluate a pill-token list.
    Chain model: [target?] addStringValue(arg, arg, ...) where every token
    after the add* function is one of its arguments (concatenated)."""
    if not tokens:
        return None
    if len(tokens) == 1 and tokens[0].get("t") == "raw":
        return eval_raw_expr(tokens[0].get("v", ""), ctx)

    def val_of(tok):
        t = tok.get("t")
        if t == "ref":
            return get_path(ctx, tok.get("v", ""))
        if t == "str":
            return tok.get("v", "")
        if t == "num":
            v = str(tok.get("v", "0"))
            try:
                return float(v) if "." in v else int(v)
            except Exception:
                return tok.get("v")
        if t == "json":
            return []
        if t == "cond":
            return eval_cond_token(tok, ctx)
        if t == "fn":
            f = tok.get("v")
            if f == "randomUUID":
                return uuid.uuid4().hex[:12]
            if f == "random":
                return random.randint(0, 99999)
            return None
        return None

    acc = None
    have = False
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        t = tok.get("t")
        if t == "fn" and tok.get("v") in ("addStringValue", "addNumberValue", "addBooleanValue"):
            args = [val_of(x) for x in tokens[i + 1:] if x.get("t") not in ("open", "close")]
            if tok.get("v") == "addNumberValue":
                try:
                    joined = sum(float(a) for a in args)
                    joined = int(joined) if joined == int(joined) else joined
                except Exception:
                    joined = "".join("" if a is None else str(a) for a in args)
            else:
                joined = "".join("" if a is None else str(a) for a in args)
            if not isinstance(acc, list):
                acc = [] if acc in (None, "") else [acc]
            acc.append(joined)
            return acc
        elif t == "fn" and tok.get("v") in ("asJSON", "toString", "format", "concat"):
            i += 1                     # chain markers; no-op on the accumulator
        elif t == "fn" and tok.get("v") == "upper":
            acc = str(acc).upper() if acc is not None else acc
            i += 1
        elif t == "fn" and tok.get("v") == "lower":
            acc = str(acc).lower() if acc is not None else acc
            i += 1
        elif t in ("open", "close"):
            i += 1
        else:
            v = val_of(tok)
            if not have:
                acc = v
                have = True
            else:
                acc = ("" if acc is None else str(acc)) + ("" if v is None else str(v))
            i += 1
    return acc


def _split_args(s):
    out, depth, quote, cur = [], 0, None, ""
    for c in s:
        if quote:
            cur += c
            if c == quote:
                quote = None
            continue
        if c in "\"'":
            quote = c
            cur += c
            continue
        if c == "(":
            depth += 1
        if c == ")":
            depth -= 1
        if c == "," and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += c
    if cur.strip():
        out.append(cur)
    return out


def _raw_scalar(s, ctx):
    s = (s or "").strip()
    if not s:
        return None
    # ternary: ((cond) valueA else valueB)  -- the MVEL-ish form the builder shows
    mt = re.match(r"^\(\s*\((.*?)\)\s*(.*?)\s+else\s+(.*)\)$", s, re.S)
    if mt:
        cond, a, b = mt.group(1).strip(), mt.group(2), mt.group(3)
        mc = re.match(r"^(.*?)\s*(==|!=)\s*(.*)$", cond, re.S)
        if mc:
            l = _raw_scalar(mc.group(1), ctx)
            r = _raw_scalar(mc.group(3), ctx)
            ok = (str(l) == str(r)) if mc.group(2) == "==" else (str(l) != str(r))
        else:
            ok = truthy(_raw_scalar(cond, ctx))
        return _raw_scalar(a if ok else b, ctx)
    # concatenation with + at top level
    parts = _split_plus(s)
    if len(parts) > 1:
        return "".join("" if (v := _raw_scalar(p, ctx)) is None else str(v) for p in parts)
    if (s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'"):
        return s[1:-1]
    if re.match(r"^-?\d+(\.\d+)?$", s):
        return float(s) if "." in s else int(s)
    if s == "{}":
        return []
    if s in ("true", "false"):
        return s == "true"
    if s == "randomUUID()":
        return uuid.uuid4().hex[:12]
    if s == "random()":
        return random.randint(0, 99999)
    if re.match(r"^(data\.)?[A-Za-z0-9_.]+$", s):
        return get_path(ctx, s[5:] if s.startswith("data.") else s)
    raise ValueError("Unsupported expression: %s" % s)


def _split_plus(s):
    out, depth, quote, cur = [], 0, None, ""
    for c in s:
        if quote:
            cur += c
            if c == quote:
                quote = None
            continue
        if c in "\"'":
            quote = c
            cur += c
            continue
        if c == "(":
            depth += 1
        if c == ")":
            depth -= 1
        if c == "+" and depth == 0:
            out.append(cur)
            cur = ""
        else:
            cur += c
    out.append(cur)
    return [p for p in out if p.strip()]


def eval_raw_expr(text, ctx):
    """Best-effort evaluation of expression (text) mode.
    Raises the same style of errors the real builder shows on bad syntax."""
    s = (text or "").strip()
    depth = 0
    for c in s:
        if c == "(":
            depth += 1
        if c == ")":
            depth -= 1
            if depth < 0:
                raise ValueError("Native parser error: ) expected but string constant found")
    if depth != 0:
        raise ValueError("Native parser error: ( expected but end of expression found")
    m = re.match(r"^(.*?)\.(addStringValue|addNumberValue)\((.*)\)\s*$", s, re.S)
    if m:
        target = _raw_scalar(m.group(1).replace(".asJSON()", ""), ctx)
        args = [_raw_scalar(a, ctx) for a in _split_args(m.group(3))]
        joined = "".join("" if a is None else str(a) for a in args)
        if not isinstance(target, list):
            target = [] if target in (None, "") else [target]
        target.append(joined)
        return target
    return _raw_scalar(s.replace(".asJSON()", ""), ctx)


# ---------------------------------------------------------------------------
# Continue On Conditions: Workday-style assertion expressions, e.g.
#   data.fetch.responseStatusCode.is2xxStatusCode()
#   data.fetch.response.countItemsAtJsonPath("$.Report_Entry").greaterThan(0)
#   data.init.workersEmployeeIds.isBlank().or(data.init.len.lessThanOrEquals(2000))
# ---------------------------------------------------------------------------
def _num(v):
    try:
        return float(v)
    except Exception:
        return 0.0

def _count_at(v, path):
    r = jsonpath(v, path)
    if isinstance(r, list):
        return len(r)
    return 0 if r is None else 1

_ASSERT_METHODS = {
    "is2xxStatusCode": lambda v, a: 200 <= int(_num(v)) < 300,
    "isBlank":         lambda v, a: v is None or str(v).strip() == "",
    "isNotBlank":      lambda v, a: not (v is None or str(v).strip() == ""),
    "isEmpty":         lambda v, a: v in (None, "", [], {}),
    "isNotEmpty":      lambda v, a: v not in (None, "", [], {}),
    "length":          lambda v, a: len(str(v)) if v is not None else 0,
    "size":            lambda v, a: len(v) if isinstance(v, (list, dict, str)) else 0,
    "countItemsAtJsonPath": lambda v, a: _count_at(v, a[0]),
    "greaterThan":         lambda v, a: _num(v) > _num(a[0]),
    "greaterThanOrEquals": lambda v, a: _num(v) >= _num(a[0]),
    "lessThan":            lambda v, a: _num(v) < _num(a[0]),
    "lessThanOrEquals":    lambda v, a: _num(v) <= _num(a[0]),
    "equals":              lambda v, a: str(v) == str(a[0]),
    "or":                  lambda v, a: truthy(v) or truthy(a[0]),
    "and":                 lambda v, a: truthy(v) and truthy(a[0]),
    "not":                 lambda v, a: not truthy(v),
}

def _assert_atom(s, ctx):
    s = s.strip()
    if (s[:1] == '"' and s[-1:] == '"') or (s[:1] == "'" and s[-1:] == "'"):
        return s[1:-1]
    if re.match(r"^-?\d+(\.\d+)?$", s):
        return float(s) if "." in s else int(s)
    if s in ("true", "false"):
        return s == "true"
    if s.startswith("(") and s.endswith(")"):
        return eval_assert(s[1:-1], ctx)
    if re.match(r"^(data\.)?[A-Za-z0-9_.!]+$", s):
        return get_path(ctx, s[5:] if s.startswith("data.") else s)
    # nested expression with method calls, e.g. data.x.length()
    return eval_assert(s, ctx)

def _split_chain(s):
    """Split 'a.b.c(x).d()' into ['a','b','c(x)','d()'] respecting parens/quotes."""
    out, depth, quote, cur = [], 0, None, ""
    for c in s:
        if quote:
            cur += c
            if c == quote:
                quote = None
            continue
        if c in "\"'":
            quote = c; cur += c; continue
        if c == "(": depth += 1
        if c == ")": depth -= 1
        if c == "." and depth == 0:
            out.append(cur); cur = ""
        else:
            cur += c
    out.append(cur)
    return out

def _split_top(s, op):
    out, depth, quote, cur, i = [], 0, None, "", 0
    while i < len(s):
        c = s[i]
        if quote:
            cur += c
            if c == quote:
                quote = None
            i += 1; continue
        if c in "\"'":
            quote = c; cur += c; i += 1; continue
        if c == "(": depth += 1
        if c == ")": depth -= 1
        if depth == 0 and s.startswith(op, i) and len(out) == 0:
            out.append(cur); cur = ""; i += len(op); continue
        cur += c; i += 1
    out.append(cur)
    return out if len(out) == 2 else [s]

def eval_assert(expr, ctx):
    s = (expr or "").strip()
    if not s:
        return True
    if s.startswith("(") and s.endswith(")") and _split_chain(s) == [s]:
        return eval_assert(s[1:-1], ctx)
    for op in ("==", "!=", ">=", "<=", ">", "<"):
        parts = _split_top(s, op)
        if len(parts) == 2:
            l, r = eval_assert(parts[0], ctx), eval_assert(parts[1], ctx)
            if op == "==": return str(l) == str(r)
            if op == "!=": return str(l) != str(r)
            if op == ">=": return _num(l) >= _num(r)
            if op == "<=": return _num(l) <= _num(r)
            if op == ">":  return _num(l) > _num(r)
            return _num(l) < _num(r)
    parts = _split_chain(s)
    i, base = 0, []
    while i < len(parts) and "(" not in parts[i]:
        base.append(parts[i]); i += 1
    val = _assert_atom(".".join(base), ctx) if base else None
    if i == len(parts) and not base:
        raise ValueError("Unsupported assertion: %s" % s)
    while i < len(parts):
        m = re.match(r"^([A-Za-z0-9_]+)\((.*)\)$", parts[i].strip(), re.S)
        if not m:
            raise ValueError("Bad method call: %s" % parts[i])
        name, args = m.group(1), [_assert_atom(a, ctx) for a in _split_args(m.group(2))]
        if name not in _ASSERT_METHODS:
            raise ValueError("Unknown function: %s()" % name)
        val = _ASSERT_METHODS[name](val, args)
        i += 1
    return val


def _short(v):
    try:
        s = json.dumps(v)
    except Exception:
        s = str(v)
    return s if len(s) <= 140 else s[:137] + "..."


def _launch_param_names(orch):
    """Launch parameters an orchestration exposes: explicit launchParams if the
    builder defined them, otherwise every Input.<name> referenced in its steps."""
    names = [p.get("name") for p in (orch.get("launchParams") or []) if p.get("name")]
    if names:
        return names
    found = re.findall(r"Input\.([A-Za-z0-9_]+)", json.dumps(orch.get("steps", [])))
    out = []
    for n in found:
        if n not in out:
            out.append(n)
    return out


def run_orchestration(orch, trigger_label="Run", inputs=None):
    ctx = {"Input": inputs or {}}
    trace = []
    out_files = []
    status = "Completed"
    error = None
    t0 = time.time()

    def log(step, typ, msg, st="Completed"):
        trace.append({"step": step, "type": typ, "status": st, "message": msg})

    def exec_steps(steps, depth=0):
        for s in steps:
            exec_one(s, depth)

    def run_loop(s, depth):
        ref = s.get("ref", "Loop")
        p = s.get("props", {}) or {}
        src = get_path(ctx, p.get("dataSetRef", "")) if p.get("dataSetRef") else None
        items = jsonpath(src, p.get("dataSetPath", "")) if p.get("dataSetPath") else src
        if not isinstance(items, list):
            items = [items] if items is not None else []
        fp = p.get("filterPath", "")
        if fp:
            items = [it for it in items if truthy(jsonpath(it, fp))]
        for sb in (p.get("sortBy") or []):
            sp = sb.get("path", "")
            if sp:
                try:
                    items = sorted(items, key=lambda it: (jsonpath(it, sp) is None, jsonpath(it, sp)),
                                   reverse=(sb.get("dir") == "desc"))
                except Exception:
                    pass
        log(ref, "Loop", "iterating %d item(s)" % len(items))
        agg = s.get("aggregation") or {}
        do_agg = bool(agg) and not agg.get("deleted")
        outputs = [_norm_out(o) for o in (agg.get("outputs") or [])] if do_agg else []
        aref = agg.get("ref", "Aggregate") if do_agg else "Aggregate"
        acc = {}        # Custom strategy accumulators, live under ctx[aref]
        started = {}    # Custom: has Initial Value fired yet for this output?
        collected = {o["name"]: [] for o in outputs}
        if do_agg:
            ctx[aref] = acc     # lets Next Value reference Aggregate.<Output>
        for i, it in enumerate(items):
            ctx["Loop"] = {"item": it, "index": i, "itemNumber": i + 1}
            ctx[ref] = ctx["Loop"]
            # Early Stop condition (optional)
            es = agg.get("earlyStop") if do_agg else None
            if es and truthy(jsonpath(it, es)):
                log(ref, "Loop", "early stop at item %d" % (i + 1))
                break
            exec_steps(s.get("body", []), depth + 1)
            for o in outputs:
                name = o["name"]
                strat = o.get("strategy", "JSON")
                try:
                    # Optional Condition gate: input aggregates only when true
                    cond_toks = o.get("condition") or []
                    if cond_toks and not truthy(agg_eval(cond_toks, ctx)):
                        log(aref, "Aggregate",
                            "%s: item %d skipped (Condition = false)" % (name, i + 1), "Skipped")
                        continue
                    if strat == "Custom":
                        if not started.get(name):
                            acc[name] = agg_eval(o.get("initialValue") or [], ctx)
                            started[name] = True
                            log(aref, "Aggregate", "%s: item %d -> Initial Value -> %s"
                                % (name, i + 1, _short(acc[name])))
                        else:
                            acc[name] = agg_eval(o.get("nextValue") or [], ctx)
                            log(aref, "Aggregate", "%s: item %d -> Next Value -> %s"
                                % (name, i + 1, _short(acc[name])))
                        ctx[aref] = acc
                    elif strat == "JSON" and (o.get("jsonFragment") or []):
                        collected[name].append(agg_eval(o.get("jsonFragment"), ctx))
                    elif strat == "CSV":
                        cols = [c.strip() for c in str(o.get("csvColumns") or "").split(",") if c.strip()]
                        row = []
                        for c in cols:
                            # bare name -> Loop.item.<name>; dotted -> full context path
                            v = get_path(ctx, c) if "." in c else (it.get(c) if isinstance(it, dict) else None)
                            row.append("" if v is None else str(v))
                        collected[name].append(row)
                    else:
                        collected[name].append(it)
                except Exception as ex:
                    log(aref, "Aggregate",
                        "%s: item %d -> Error in expression: %s" % (name, i + 1, ex), "Error")
        if do_agg:
            result = {}
            for o in outputs:
                name = o["name"]
                strat = o.get("strategy", "JSON")
                if strat == "Custom":
                    val = acc.get(name)
                    if o.get("outputType") == "Text" and isinstance(val, list):
                        val = "\n".join(str(v) for v in val)
                    result[name] = val
                elif strat == "Text":
                    result[name] = "\n".join(str(v) for v in collected.get(name, []))
                elif strat == "Count":
                    result[name] = len(collected.get(name, []))
                elif strat == "CSV":
                    import csv as _csv
                    import io as _io
                    buf = _io.StringIO()
                    w = _csv.writer(buf, delimiter=(str(o.get("csvDelimiter") or ",") + ",")[0],
                                    quotechar='"', lineterminator="\n")
                    if o.get("csvHeader"):
                        w.writerow([c.strip().split(".")[-1] for c in str(o.get("csvColumns") or "").split(",") if c.strip()])
                    for row in collected.get(name, []):
                        w.writerow(row)
                    result[name] = buf.getvalue()
                else:
                    result[name] = collected.get(name, [])
            ctx[aref] = result
            if agg.get("failWhenNoInputs") and not items:
                raise ValueError("Aggregate '%s': no inputs" % aref)
            summary = " | ".join("%s = %s" % (o["name"], _short(result.get(o["name"])))
                                 for o in outputs)
            log(aref, "Aggregate", summary or "no outputs")

    def exec_one(s, depth=0):
        typ = s.get("type")
        ref = s.get("ref", typ)
        p = s.get("props", {}) or {}

        if typ == "send-workday-raas-request":
            prefix = p.get("urlPrefix", "") or ""
            path = p.get("path", "")
            if isinstance(path, list):
                path = resolve_tokens(path, ctx)
            full_url = prefix + str(path or "")
            # Query Parameters (RaaS prompts, e.g. Workers!Employee_ID=21001!21002)
            pairs = []
            for q in (p.get("queryParams") or []):
                k = (q.get("key") or "").strip()
                v = q.get("value", "")
                if isinstance(v, list):
                    v = resolve_tokens(v, ctx)
                v = subst(str(v if v is not None else ""), ctx)
                if k and v != "" and not re.match(r"^\{[A-Za-z0-9_.]+\}$", v):
                    pairs.append("%s=%s" % (urllib.parse.quote(k, safe="!"), urllib.parse.quote(v, safe="!")))
            if pairs:
                full_url = full_url + ("&" if "?" in full_url else "?") + "&".join(pairs)
            res = fetch_report(full_url)
            ctx[ref] = {"response": res["response"], "responseHeaders": res["responseHeaders"],
                        "responseStatusCode": res["responseStatusCode"]}
            if res.get("error"):
                log(ref, typ, "GET %s -> ERROR: %s" % (full_url, res["error"]), "Error")
            else:
                n = len(res["response"]) if isinstance(res["response"], list) else None
                log(ref, typ, "GET %s -> %s%s" % (full_url, res["responseStatusCode"],
                    (" (%d rows)" % n) if n is not None else " (data)"))

        elif typ == "send-http-request":
            url = p.get("url", "") or ""
            if isinstance(url, list):
                url = resolve_tokens(url, ctx)
            url = subst(str(url), ctx)
            method = (p.get("method", "GET") or "GET").upper()
            # append Query Parameters (Key/Value pairs) to the URL
            pairs = []
            for q in (p.get("queryParams") or []):
                k = (q.get("key") or "").strip()
                v = q.get("value", "")
                if isinstance(v, list):
                    v = resolve_tokens(v, ctx)
                v = subst(str(v), ctx)
                if k:
                    pairs.append("%s=%s" % (urllib.parse.quote(k), urllib.parse.quote(v)))
            if pairs:
                url = url + ("&" if "?" in url else "?") + "&".join(pairs)
            if method == "GET":
                res = fetch_report(url)
            else:
                # body can come from a step ref (bodyRef), a token list, or a literal
                if p.get("bodyRef"):
                    body = get_path(ctx, p.get("bodyRef"))
                elif isinstance(p.get("body"), list):
                    body = resolve_tokens(p.get("body"), ctx)
                else:
                    body = subst(p.get("body", "") or "", ctx)
                if isinstance(body, (dict, list)):
                    body = json.dumps(body)
                res = http_send(url, method, str(body if body is not None else ""),
                                p.get("contentType", "application/json"))
            ctx[ref] = {"response": res["response"], "responseHeaders": res["responseHeaders"],
                        "responseStatusCode": res["responseStatusCode"]}
            if res.get("error"):
                log(ref, typ, "%s %s -> ERROR: %s" % (method, url, res["error"]), "Error")
            else:
                log(ref, typ, "%s %s -> %s" % (method, url, res["responseStatusCode"]))

        elif typ in ("send-paged-http-request", "send-workday-api-request",
                     "send-paged-workday-rest-call", "send-paged-workday-soap-call", "send-prism-request"):
            ctx[ref] = {"response": {"ok": True}, "responseStatusCode": 200}
            log(ref, typ, "request sent -> 200")

        elif typ == "store-document":
            doc = resolve_tokens(p.get("documentToStore", []), ctx)
            title = resolve_tokens(p.get("documentTitle", []), ctx) or "Document"
            body = render_txt(doc) if isinstance(doc, (list, dict)) else str(doc if doc is not None else "")
            ftype = "CSV Document (CSV)" if str(title).lower().endswith(".csv") else "Text Document (TXT)"
            out_files.append({"title": title, "type": ftype, "body": body})
            log(ref, typ, "stored '%s' (%d bytes)" % (title, len(body)))

        elif typ == "create-text-template":
            msg = subst(p.get("message", "") or "", ctx)
            try:
                val = json.loads(msg) if msg.strip()[:1] in "[{" else msg
            except Exception:
                val = msg
            ctx[ref] = {"message": val, "contentType": p.get("contentType", "application/json")}
            log(ref, typ, "text template created (%s)" % p.get("contentType", ""))

        elif typ in ("create-values", "create-json"):
            out = {}
            for v in (p.get("values") or []):
                name = v.get("name") or "value"
                src = get_path(ctx, v.get("sourceRef", "")) if v.get("sourceRef") else None
                out[name] = jsonpath(src, v.get("jsonPath", "")) if v.get("jsonPath") else src
            if out and "value" not in out:
                out["value"] = next(iter(out.values()))
            ctx[ref] = out or {"value": None}
            summ = ", ".join("%s=%s" % (k, out[k]) for k in out if k != "value")
            log(ref, typ, "extracted " + (summ or "(no mappings)"))

        elif typ == "validate":
            log(ref, typ, "validation passed")

        elif typ in ("loop", "batch-loop", "join-loop"):
            run_loop(s, depth)

        elif typ == "log":
            cond = p.get("condition", "true")
            if truthy(cond):
                if p.get("messageRef"):
                    val = get_path(ctx, p.get("messageRef"))
                else:
                    val = p.get("message", "")
                log(ref, "LogStep", str(val))
            else:
                log(ref, "LogStep", "(skipped: condition false)", "Skipped")

        elif typ == "continue-on-conditions":
            # Every assertion must be true, otherwise the orchestration fails
            # with the assertion's message (real Orchestrate behavior).
            failed = None
            n = 0
            for a in (p.get("assertions") or []):
                expr = (a.get("expr") or "").strip()
                if not expr:
                    continue
                n += 1
                try:
                    ok = truthy(eval_assert(expr, ctx))
                except Exception as ex:
                    raise ValueError("%s: assertion error in '%s': %s" % (ref, expr, ex))
                if not ok:
                    failed = subst(a.get("message") or ("Assertion failed: " + expr), ctx)
                    break
            if failed:
                log(ref, typ, failed, "Error")
                raise ValueError("%s: %s" % (ref, failed))
            log(ref, typ, "all %d assertion(s) passed" % n)
            exec_steps(s.get("body", []), depth + 1)

        elif typ == "branch-on-conditions":
            log(ref, typ, "evaluated condition")
            exec_steps(s.get("body", []), depth + 1)

        elif typ == "send-integration-message":
            sev = (p.get("severity") or "INFO").upper()
            msg = subst(p.get("summary") or "", ctx)
            log(ref, "IntegrationMessage", "[%s] %s" % (sev, msg),
                "Error" if sev == "ERROR" else "Completed")

        elif typ in ("trigger-business-process", "trigger-integration", "trigger-pdf-generation"):
            log(ref, typ, "triggered")

        elif typ in ("put-amazon-eventbridge-event", "invoke-aws-lambda-function"):
            ctx[ref] = {"response": {"ok": True}}
            log(ref, typ, "AWS call ok")

        else:
            log(ref, typ, "executed")

    try:
        exec_steps(orch.get("steps", []))
    except Exception as e:
        status = "Error"
        error = str(e)
        # Global Error Handler (Orchestrate "_globalErrorHandler"): one ERROR
        # integration message with processingError.message() + location.
        loc = trace[-1]["step"] if trace else "Start"
        ctx["_orchestration"] = {"processingError": {"message": error, "locationId": loc,
                                                     "locationPath": "/%s" % loc}}
        log("_globalErrorHandler", "GlobalErrorHandler",
            "Integration failed: %s | Error occurred at: %s" % (error, loc), "Error")

    return {
        "status": status,
        "error": error,
        "durationMs": int((time.time() - t0) * 1000),
        "trace": trace,
        "outputFiles": out_files,
        "context": ctx,
    }

def render_txt(data):
    """Render RaaS rows as a tab-delimited text document."""
    if isinstance(data, list) and data and isinstance(data[0], dict):
        cols = list(data[0].keys())
        lines = ["\t".join(cols)]
        for row in data:
            lines.append("\t".join(str(row.get(c, "")) for c in cols))
        return "\n".join(lines)
    return json.dumps(data, indent=2, default=str)


# ===========================================================================
# Icons (inline SVG) for fidelity
# ===========================================================================
IC_PLUG = ('<svg viewBox="0 0 24 24" width="38" height="38" fill="none" stroke="#5a6472" '
           'stroke-width="1.6"><path d="M8 3v5M16 3v5M6 8h12v3a6 6 0 0 1-12 0V8z"/>'
           '<path d="M12 17v4"/></svg>')
IC_MEGA = ('<svg viewBox="0 0 24 24" width="40" height="40" fill="none" stroke="#5a6472" '
           'stroke-width="1.6"><path d="M3 11v2a1 1 0 0 0 1 1h2l9 5V6L6 11H4a1 1 0 0 0-1 0z"/>'
           '<path d="M18 9a3 3 0 0 1 0 6"/></svg>')
IC_DB = ('<svg viewBox="0 0 24 24" width="15" height="15" fill="#fff"><ellipse cx="12" cy="5" rx="8" ry="3"/>'
         '<path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>')
IC_BRACKETS = ('<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="#fff" stroke-width="2.2">'
               '<path d="M9 4H6v16h3M15 4h3v16h-3"/></svg>')
IC_BRANCH = ('<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="#fff" stroke-width="2">'
             '<path d="M6 3v6M6 9a6 6 0 0 0 6 6h6M18 11l3-2-3-2M18 19l3-2-3-2"/></svg>')

# Category icons (small, colored) for the palette headers
CAT_DB = ('<svg viewBox="0 0 24 24" width="16" height="16" fill="#2f9e44"><ellipse cx="12" cy="5" rx="8" ry="3"/>'
          '<path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>')
CAT_BR = ('<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="#2563eb" stroke-width="2.2">'
          '<path d="M9 4H6v16h3M15 4h3v16h-3"/></svg>')
CAT_LOGIC = ('<svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="#d9730d" stroke-width="2">'
             '<path d="M6 3v6M6 9a6 6 0 0 0 6 6h6M18 11l3-2-3-2M18 19l3-2-3-2"/></svg>')


# ===========================================================================
# Shared chrome - Developer Site (white)
# ===========================================================================
DEV_HEAD = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>__TITLE__</title>
<style>
 *{box-sizing:border-box;margin:0;padding:0}
 body{font-family:'Segoe UI',Roboto,Arial,sans-serif;background:#fff;color:#1c1f23;font-size:14px}
 a{color:#2557d6;text-decoration:none} a:hover{text-decoration:underline}
 .dev-top{display:flex;align-items:center;gap:18px;padding:14px 22px;border-bottom:1px solid #e7e9ee}
 .wlogo{width:36px;height:36px;border-radius:50%;background:#0a2540;color:#f5b700;display:flex;
        align-items:center;justify-content:center;font-weight:800;font-size:18px}
 .dev-top .brand{color:#2557d6;font-weight:700;font-size:20px}
 .dev-top .ham{color:#5a6472;font-size:22px;cursor:pointer}
 .dev-search{flex:1;max-width:860px;background:#eef1f5;border-radius:8px;padding:11px 16px;color:#7a828c}
 .dev-top .who{display:flex;align-items:center;gap:8px;color:#5a6472;font-weight:600;padding-left:18px;border-left:1px solid #e7e9ee}
 .layout{display:flex;min-height:calc(100vh - 66px)}
 .side{width:262px;border-right:1px solid #eef1f5;padding:26px 12px}
 .side a{display:flex;align-items:center;gap:14px;padding:12px 16px;border-radius:10px;color:#2b2f36;font-weight:600}
 .side a.active{background:#eef3ff;color:#2557d6}
 .side a:hover{background:#f5f7fb;text-decoration:none}
 .side .ic{width:22px;color:#5a6472}
 .main{flex:1;padding:26px 36px;max-width:1340px}
 .crumb{color:#5a6472;font-size:14px;margin-bottom:18px}
 .crumb a{color:#2557d6}
 h1{font-size:30px;margin-bottom:4px}
 h2{font-size:20px;margin-bottom:14px}
 .banner{background:linear-gradient(90deg,#1f4fc4,#f5a623);color:#fff;text-align:center;
         padding:11px;font-weight:600;display:flex;align-items:center;justify-content:center;gap:18px}
 .banner .lm{background:#fff;color:#1c1f23;border-radius:18px;padding:5px 14px;font-size:13px}
 .cards{display:grid;grid-template-columns:repeat(3,1fr);gap:20px;margin:26px 0}
 .bigcard{border:1px solid #e7e9ee;border-radius:14px;padding:22px;display:flex;align-items:center;gap:16px;cursor:pointer}
 .bigcard:hover{border-color:#2557d6;box-shadow:0 2px 10px rgba(37,87,214,.08)}
 .bigcard .t{font-size:18px;font-weight:700} .bigcard .s{color:#6b727c;font-size:13px}
 .bigcard .arr{margin-left:auto;color:#9aa3b0;font-size:22px}
 .panel{border:1px solid #e7e9ee;border-radius:14px;padding:22px;margin-bottom:20px}
 .tabs{display:flex;gap:26px;border-bottom:1px solid #e7e9ee;margin-bottom:22px}
 .tabs a{padding:12px 2px;color:#5a6472;font-weight:600;border-bottom:3px solid transparent}
 .tabs a.active{color:#2557d6;border-bottom-color:#2557d6}
 .twocol{display:grid;grid-template-columns:1.7fr 1fr;gap:24px}
 .addlink{color:#2557d6;font-weight:600;display:inline-flex;align-items:center;gap:6px;margin-top:14px}
 .meta .k{font-size:13px;color:#8a909a;margin-top:16px} .meta .v{font-size:15px}
 .btn{display:inline-block;border:1px solid #c4ccd6;background:#fff;border-radius:22px;padding:9px 20px;
      cursor:pointer;font-size:14px;font-weight:600}
 .btn.pri{background:#2557d6;border-color:#2557d6;color:#fff}
 .btn:hover{background:#f3f6fb} .btn.pri:hover{background:#1f49b8}
 .modal-bg{position:fixed;inset:0;background:rgba(20,30,50,.45);display:flex;align-items:center;justify-content:center;z-index:60}
 .modal{background:#fff;border-radius:14px;padding:30px;width:560px;max-width:92vw}
 .modal h3{font-size:24px;margin-bottom:18px}
 .modal label{display:block;font-weight:600;margin-bottom:6px}
 .modal input{width:100%;border:1px solid #2557d6;border-radius:8px;padding:11px 12px;font-size:15px}
 .row2{display:flex;gap:12px;margin-top:20px}
 .recent{border:1px solid #e7e9ee;border-radius:14px;padding:20px}
 .recent .item{display:flex;align-items:center;gap:10px;padding:14px 4px;border-bottom:1px solid #f0f2f6}
 .recent .ok{color:#1f9d55} .recent .sub{color:#8a909a;font-size:13px}
 .sel{border:2px solid #2557d6 !important}
</style></head><body>"""

DEV_NAV = """
<div class="dev-top">
  <div class="wlogo">W</div>
  <div class="brand">Developers</div>
  <div class="ham">&#9776;</div>
  <div class="dev-search">&#128269;&nbsp; Search Developer Site (/)</div>
  <div class="who">&#128100; WCP</div>
</div>"""


def dev_side(active):
    items = [("Apps", "/orchestrate/apps", "&#128462;"),
             ("Analytics", "#", "&#128200;"),
             ("Tenants", "#", "&#128451;"),
             ("API Clients", "#", "&#128274;"),
             ("Users", "#", "&#128101;"),
             ("Third Party Integrations", "#", "&#128279;")]
    h = '<div class="side">'
    for name, url, ic in items:
        cls = " active" if name == active else ""
        h += '<a class="%s" href="%s"><span class="ic">%s</span>%s</a>' % (cls, url, ic, name)
    return h + "</div>"


def dev_page(title, body, nav=True):
    html = DEV_HEAD.replace("__TITLE__", title)
    if nav:
        html += DEV_NAV
    return Response(html + body + "</body></html>", mimetype="text/html")


# ===========================================================================
# Developer Site: Home
# ===========================================================================
@app.route("/orchestrate")
def dev_home():
    d = load()
    recents = ""
    names = list(d["apps"].values())
    show = (names + [{"name": "localDisk"}, {"name": "ParkingRegistration"}])[:3]
    for i, a in enumerate(show):
        link = ("/orchestrate/console/apps/view/%s" % a["id"]) if a.get("id") else "#"
        recents += ('<div class="item"><span class="ok">&#10004;</span>'
                    '<div><a href="%s"><b>%s</b></a>'
                    '<div class="sub">Last Modified %s by Tony Gilfillan</div></div></div>'
                    % (link, esc(a["name"]), "a few seconds ago" if i == 0 else "an hour ago"))
    body = """
    <div class="banner">Workday DevCon | June 1-4, 2026 | Resorts World Las Vegas | Registration Open Now.
      <span class="lm">Learn More &#8599;</span></div>
    <div class="main" style="max-width:1340px;margin:0 auto">
      <h1 style="margin:26px 0 0">Hi Tony! Welcome to the Developer Site.</h1>
      <div class="cards">
        <div class="bigcard"><span style="font-size:30px">&#128202;</span>
          <div><div class="t">Build an Extend App</div><div class="s">With App Builder</div></div>
          <span class="arr">&#8250;</span></div>
        <div class="bigcard" onclick="location='/orchestrate/build-app'">
          <span style="font-size:30px">&#128719;</span>
          <div><div class="t">Build an Integration App</div><div class="s">With Orchestration Builder</div></div>
          <span class="arr">&#8250;</span></div>
        <div class="bigcard" onclick="location='/orchestrate/apps'">
          <span style="font-size:30px">&#128187;</span>
          <div><div class="t">Manage Apps</div><div class="s">See all apps in your company.</div></div>
          <span class="arr">&#8250;</span></div>
      </div>
      <div class="twocol">
        <div>
          <h2>Pick Up Where You Left Off</h2>
          <div class="recent"><div style="font-weight:700;margin-bottom:6px">Recently Modified</div>__RECENT__</div>
        </div>
        <div>
          <h2>What's New</h2>
          <div class="recent">
            <div style="font-weight:700;color:#2557d6;margin-bottom:10px">&#128240; Product Updates</div>
            <div style="padding:8px 0"><a href="#">&#128640; Workday Developer CLI - Now Generally Available</a></div>
            <div style="padding:8px 0"><a href="#">&#128640; Orchestrate - 48 Hour Runtime</a></div>
            <div style="padding:8px 0"><a href="#">Orchestrate - Pagination for External REST APIs</a></div>
          </div>
        </div>
      </div>
    </div>""".replace("__RECENT__", recents)
    return dev_page("Developer Site", body, nav=False)


# ===========================================================================
# Build an Integration App modal
# ===========================================================================
@app.route("/orchestrate/build-app")
def build_app():
    body = """
    <div class="modal-bg" onclick="if(event.target===this)location='/orchestrate'">
      <div class="modal" style="width:760px">
        <div style="display:flex;align-items:center"><h3 style="flex:1;font-size:22px">
          Build an Integration App <span style="color:#6b727c;font-weight:400">with Orchestration Builder</span></h3>
          <a href="/orchestrate" style="font-size:22px;color:#5a6472">&#10005;</a></div>
        <p style="color:#5a6472;margin-bottom:18px">Orchestration Builder enables you to author Integration apps
          on the Workday Developer Site. <a href="#">Learn More</a></p>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:18px">
          <div>
            <div style="font-weight:700;margin-bottom:10px">Create App</div>
            <div class="bigcard" style="margin-bottom:12px"><span style="font-size:22px">&#128214;</span>
              <div class="t" style="font-size:15px">Copy from App Catalog</div><span class="arr">&#8250;</span></div>
            <div class="bigcard" style="margin-bottom:12px"><span style="font-size:22px">&#11014;</span>
              <div class="t" style="font-size:15px">Upload a Zip File</div><span class="arr">&#8250;</span></div>
            <div class="bigcard" onclick="location='/orchestrate/create-app'"><span style="font-size:22px">&#128736;</span>
              <div class="t" style="font-size:15px">Start from Scratch</div><span class="arr">&#8250;</span></div>
          </div>
          <div>
            <div style="font-weight:700;margin-bottom:10px">Open App</div>
            <div class="bigcard" style="margin-bottom:12px" onclick="location='/orchestrate/apps'">
              <span style="font-size:22px">&#128194;</span>
              <div class="t" style="font-size:15px">Open from App Hub</div><span class="arr">&#8250;</span></div>
            <div class="bigcard"><span style="font-size:22px">&#128193;</span>
              <div class="t" style="font-size:15px">Open a Local Folder</div></div>
          </div>
        </div>
      </div>
    </div>"""
    return dev_page("Build an Integration App", body, nav=False)


@app.route("/orchestrate/create-app", methods=["GET", "POST"])
def create_app():
    if request.method == "POST":
        d = load()
        name = request.form.get("name", "New App").strip()
        ref = re.sub(r"\W+", "", name)[:24] or "app"
        aid = "%s_%s" % (ref, _ref_suffix())
        d["apps"][aid] = {"id": aid, "name": name, "refId": aid,
                          "appId": _ref_suffix() + _ref_suffix(),
                          "description": request.form.get("description", "--") or "--",
                          "created": datetime.date.today().strftime("%m/%d/%Y"),
                          "createdBy": "Tony Gilfillan", "orchestrations": {}}
        save(d)
        return Response("", status=302, headers={"Location": "/orchestrate/console/apps/view/%s" % aid})
    suffix = "_" + _ref_suffix()
    body = """
    <div class="modal-bg" onclick="if(event.target===this)location='/orchestrate/build-app'">
      <div class="modal">
        <h3>Create an App <span style="color:#6b727c;font-weight:400">from Scratch</span></h3>
        <div style="color:#8a909a;font-weight:600">Company</div><div style="margin-bottom:14px">WCP</div>
        <label>Name <span style="color:#d33">*</span></label>
        <input id="nm" value="Display Name For Integration"
          oninput="document.getElementById('rid').value=this.value.replace(/[^A-Za-z0-9]/g,'')">
        <label style="margin-top:16px">Reference ID <span style="color:#d33">*</span></label>
        <div style="display:flex;align-items:center;gap:8px">
          <input id="rid" value="displayNameForIntegration"><span style="color:#8a909a">__SUF__</span></div>
        <label style="margin-top:16px">Description</label>
        <textarea id="ds" style="width:100%;border:1px solid #c4ccd6;border-radius:8px;padding:10px;min-height:80px"
          placeholder="Optional"></textarea>
        <div class="row2">
          <button class="btn pri" onclick="go()">Create and Edit</button>
          <button class="btn" onclick="go()">Create and Go to Overview</button>
        </div>
      </div>
    </div>
    <script>
    function go(){var f=document.createElement('form');f.method='POST';f.action='/orchestrate/create-app';
      f.innerHTML='<input name=name value="'+document.getElementById('nm').value.replace(/"/g,'&quot;')+'">'+
        '<input name=description value="'+document.getElementById('ds').value.replace(/"/g,'&quot;')+'">';
      document.body.appendChild(f);f.submit();}
    </script>""".replace("__SUF__", suffix)
    return dev_page("Create an App", body, nav=False)


# ===========================================================================
# Apps list + App overview
# ===========================================================================
@app.route("/orchestrate/apps")
def apps_list():
    d = load()
    rows = ""
    for a in d["apps"].values():
        rows += ('<div class="recent"><div class="item" style="border:none">'
                 '<a href="/orchestrate/console/apps/view/%s"><b>%s</b></a></div></div>'
                 % (a["id"], esc(a["name"])))
    body = ('<div class="layout">%s<div class="main"><div class="crumb">'
            '<a href="/orchestrate">Home</a> &#8250; Apps</div><h1>Apps</h1>'
            '<div style="margin-top:18px">%s</div>'
            '<button class="btn pri" style="margin-top:18px" onclick="location=\'/orchestrate/build-app\'">'
            '+ Build an Integration App</button></div></div>'
            % (dev_side("Apps"), rows or "<p>No apps yet.</p>"))
    return dev_page("Apps", body)


@app.route("/orchestrate/console/apps/view/<app_id>")
def app_overview(app_id):
    d = load()
    a = d["apps"].get(app_id)
    if not a:
        return dev_page("Not found", "<div class='main'>App not found.</div>")
    orchs = ""
    for o in a["orchestrations"].values():
        orchs += ('<div class="item"><a href="/orchestrate/app/%s/development/orchestrations/%s"><b>%s</b></a>'
                  '<span style="margin-left:auto;color:#9aa3b0">&#8942;</span></div>'
                  % (a["appId"], o["name"], esc(o["name"])))
    body = """
    <div class="layout">__SIDE__
      <div class="main">
        <div class="crumb"><a href="/orchestrate">Home</a> &#8250; <a href="/orchestrate/apps">Apps</a> &#8250; __NAME__</div>
        <h1 style="display:flex;align-items:center;gap:12px">__NAME__
          <span style="border:1px solid #e7e9ee;border-radius:50%;width:42px;height:42px;display:inline-flex;
            align-items:center;justify-content:center;font-size:20px">&#8230;</span></h1>
        <div class="tabs" style="margin-top:18px">
          <a class="active">Overview</a><a>Promotions</a><a>Activity</a>
          <a>Orchestration Activity</a><a>Logs</a></div>
        <div class="twocol">
          <div>
            <div class="panel">
              <h2>Orchestrations</h2>
              <div>__ORCHS__</div>
              <a class="addlink" href="/orchestrate/app/__APPID__/orchestrations">+ Add Orchestration</a>
            </div>
            <div class="panel">
              <h2>Tenant Configuration</h2>
              <p style="color:#6b727c">No Tenant Configuration</p>
              <a class="addlink" href="#">+ Add Tenant Configuration</a>
            </div>
          </div>
          <div>
            <div class="panel meta">
              <h2>About</h2>
              <div class="k">Description</div><div class="v">__DESC__</div>
              <div class="k">Reference ID</div><div class="v">__REF__</div>
              <div class="k">Created</div><div class="v">__CREATED__ by __BY__</div>
            </div>
          </div>
        </div>
      </div>
    </div>"""
    body = (body.replace("__SIDE__", dev_side("Apps")).replace("__NAME__", esc(a["name"]))
            .replace("__ORCHS__", orchs or "<p style='color:#6b727c'>No orchestrations yet.</p>")
            .replace("__APPID__", a["appId"]).replace("__DESC__", esc(a["description"]))
            .replace("__REF__", esc(a["refId"])).replace("__CREATED__", a["created"])
            .replace("__BY__", esc(a["createdBy"])))
    return dev_page(a["name"] + " - Apps", body)


def _app_by_appid(d, appid):
    for a in d["apps"].values():
        if a.get("appId") == appid:
            return a
    return None


# ===========================================================================
# Create Orchestration (type selection + name modal)
# ===========================================================================
@app.route("/orchestrate/app/<appid>/orchestrations")
def create_orch_page(appid):
    d = load()
    a = _app_by_appid(d, appid)
    nm = a["name"] if a else "app"
    body = """
    <div style="min-height:100vh;background:#eef2f7;position:relative;overflow:hidden">
      <div class="dev-top" style="background:#fff"><div class="wlogo">W</div></div>
      <div style="padding:30px 40px"><div class="crumb"><a href="/orchestrate/apps">My Apps</a> &#8250; <b>__NM__</b></div>
        <div style="font-size:34px;margin:20px 0">__NM__</div></div>
      <div style="position:absolute;left:0;top:200px;width:560px;height:560px;border-radius:50%;
        background:radial-gradient(circle at 60% 40%,#9cc0ff,#3b82f6);opacity:.5"></div>
      <div style="max-width:760px;margin:0 auto;padding:0 30px 60px;position:relative">
        <h1 style="font-size:34px">Create Orchestration</h1>
        <p style="color:#5a6472;margin:10px 0 28px">Select from the following Orchestration types as your starting point.</p>
        <div style="color:#8a909a;font-weight:700;letter-spacing:.5px;margin-bottom:14px">BUILD YOUR OWN</div>
        <div class="bigcard" style="margin-bottom:16px;background:#fff" onclick="open_modal('Synchronous Orchestration')">
          <span style="font-size:30px;color:#2557d6">&#127939;</span>
          <div><div class="t">Synchronous Orchestration</div>
            <div class="s">Create an Orchestration that starts and completes without awaiting any other input.</div></div></div>
        <div class="bigcard" style="margin-bottom:28px;background:#fff" onclick="open_modal('Asynchronous Orchestration')">
          <span style="font-size:30px;color:#2557d6">&#9749;</span>
          <div><div class="t">Asynchronous Orchestration</div>
            <div class="s">Create an Orchestration that can await input from other processes.</div></div></div>
        <div style="color:#8a909a;font-weight:700;letter-spacing:.5px;margin-bottom:14px">REQUEST FROM WORKDAY</div>
        <div class="bigcard" style="margin-bottom:16px;background:#fff" onclick="open_modal('Workday Business Process')">
          <span style="font-size:30px;color:#2557d6">&#9685;</span>
          <div><div class="t">Workday Business Process</div>
            <div class="s">Create an Orchestration that's triggered by an existing Workday business process.</div></div></div>
        <div class="bigcard" style="margin-bottom:16px;background:#fff" onclick="open_modal('Workday Home Card')">
          <span style="font-size:30px;color:#2557d6">&#9685;</span>
          <div><div class="t">Workday Home Card</div>
            <div class="s">Create an Orchestration that adds data to a custom Workday Home card.</div></div></div>
        <div class="bigcard sel" style="background:#fff" onclick="open_modal('Workday Integration System')">
          <span style="font-size:30px;color:#2557d6">&#9685;</span>
          <div><div class="t">Workday Integration System</div>
            <div class="s">Create an orchestration that is triggered from a Workday integration system.</div></div></div>
      </div>
    </div>
    <div class="modal-bg" id="mb" style="display:none">
      <div class="modal">
        <div style="display:flex"><h3 style="flex:1">Create New Orchestration</h3>
          <span onclick="document.getElementById('mb').style.display='none'"
            style="color:#2557d6;font-size:22px;cursor:pointer">&#10005;</span></div>
        <label>Name</label><input id="onm" placeholder="raas">
        <div class="row2"><button class="btn pri" onclick="done()">Done</button>
          <button class="btn" onclick="document.getElementById('mb').style.display='none'">Cancel</button></div>
      </div>
    </div>
    <script>
    var OTYPE='Workday Integration System';
    function open_modal(t){OTYPE=t;document.getElementById('mb').style.display='flex';document.getElementById('onm').focus();}
    function done(){var n=document.getElementById('onm').value.trim()||'raas';
      var f=document.createElement('form');f.method='POST';
      f.action='/orchestrate/app/__APPID__/orchestrations/create';
      f.innerHTML='<input name=name value="'+n.replace(/"/g,'')+'"><input name=type value="'+OTYPE+'">';
      document.body.appendChild(f);f.submit();}
    </script>""".replace("__NM__", esc(nm)).replace("__APPID__", appid)
    return dev_page("Create Orchestration", body, nav=False)


@app.route("/orchestrate/app/<appid>/orchestrations/create", methods=["POST"])
def create_orch(appid):
    d = load()
    a = _app_by_appid(d, appid)
    name = request.form.get("name", "raas").strip() or "raas"
    type_str = request.form.get("type", "Workday Integration System")
    start_map = {"Synchronous Orchestration": "synchronous", "Synchronous": "synchronous",
                 "Asynchronous Orchestration": "asynchronous", "Asynchronous": "asynchronous",
                 "Workday Business Process": "business-process",
                 "Workday Home Card": "home-card",
                 "Workday Integration System": "integration"}
    start = start_map.get(type_str, "integration")
    if start == "integration":
        steps = [{"id": "s1", "type": "send-workday-raas-request",
                  "ref": "SendWorkdayRaaSRequest",
                  "props": {"method": "GET",
                            "urlPrefix": "http://localhost:8443/task/view-report?name=",
                            "path": "", "auth": "Default Workday API Credential",
                            "contentType": "Any"}}]
    else:
        steps = []   # real builder starts with an empty canvas (Start + End only)
    if a:
        a["orchestrations"][name] = {"name": name, "type": type_str, "startType": start,
                                     "lastBuild": None, "steps": steps}
        save(d)
    return Response("", status=302,
                    headers={"Location": "/orchestrate/app/%s/development/orchestrations/%s" % (appid, name)})


# ===========================================================================
# ORCHESTRATION BUILDER
# ===========================================================================
@app.route("/orchestrate/app/<appid>/development/orchestrations/<name>")
def builder(appid, name):
    d = load()
    a = _app_by_appid(d, appid)
    if not a or name not in a["orchestrations"]:
        return dev_page("Not found", "<div class='main'>Orchestration not found.</div>")
    orch = a["orchestrations"][name]
    payload = {"appId": appid, "appName": a["name"], "orch": orch,
               "tenants": TENANTS, "buildNo": d["counters"]["build"] + 1}
    html = (BUILDER_HEAD + BUILDER_BODY.replace("__DATA__", json.dumps(payload)))
    return Response(html, mimetype="text/html")


@app.route("/orchestrate/api/save", methods=["POST"])
def api_save():
    data = request.get_json(force=True)
    d = load()
    a = _app_by_appid(d, data["appId"])
    if a:
        a["orchestrations"][data["orch"]["name"]] = data["orch"]
        save(d)
    return {"ok": True, "savedAt": datetime.datetime.now().strftime("%I:%M %p")}


@app.route("/orchestrate/api/validate", methods=["POST"])
def api_validate():
    orch = request.get_json(force=True).get("orch", {})
    issues = []
    for s in orch.get("steps", []):
        if s["type"] == "store-document" and not s.get("props", {}).get("documentToStore"):
            issues.append("%s: Document to Store is required" % s.get("ref"))
        if s["type"] == "store-document" and not s.get("props", {}).get("documentTitle"):
            issues.append("%s: Document Title is required" % s.get("ref"))
    return {"ok": len(issues) == 0, "issues": issues}


@app.route("/orchestrate/api/build", methods=["POST"])
def api_build():
    data = request.get_json(force=True)
    d = load()
    a = _app_by_appid(d, data["appId"])
    d["counters"]["build"] += 1
    bn = d["counters"]["build"]
    if a and data["orch"]["name"] in a["orchestrations"]:
        a["orchestrations"][data["orch"]["name"]] = data["orch"]
        a["orchestrations"][data["orch"]["name"]]["lastBuild"] = bn
    save(d)
    return {"ok": True, "buildNo": bn, "tenants": TENANTS}


@app.route("/orchestrate/api/run", methods=["POST"])
def api_run():
    orch = request.get_json(force=True).get("orch", {})
    res = run_orchestration(orch, "Run Logs")
    return res


@app.route("/orchestrate/api/deploy", methods=["POST"])
def api_deploy():
    data = request.get_json(force=True)
    d = load()
    a = _app_by_appid(d, data["appId"])
    tenant = data.get("tenant")
    name = data["orch"]["name"]
    # register a tenant Integration System auto-wired to the orchestration
    d["counters"]["is"] += 1
    isid = "IS%03d" % d["counters"]["is"]
    d["integration_systems"][isid] = {
        "id": isid,
        "systemName": (data.get("systemName") or (name + " demo")).strip(),
        "systemId": re.sub(r"[^A-Za-z0-9_]", "", data.get("systemName") or (name + "demo")),
        "template": "Orchestrate Integration Template", "tenant": tenant,
        "orchestrationName": name, "appReferenceId": a["refId"] if a else name + "_x",
    }
    d["deployments"].append({"tenant": tenant, "orch": name, "isId": isid,
                             "at": datetime.datetime.now().isoformat(timespec="seconds")})
    save(d)
    return {"ok": True, "isId": isid, "tenant": tenant}


@app.route("/orchestrate/api/tenants")
def api_tenants():
    return {"tenants": TENANTS}


# ===========================================================================
# TENANT SIDE - View Integration System / Launch / Background Process / Output
# ===========================================================================
TEN_HEAD = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>__TITLE__</title>
<style>
 *{box-sizing:border-box;margin:0;padding:0}
 body{font-family:'Segoe UI',Roboto,Arial,sans-serif;background:#fff;color:#1c1f23;font-size:14px}
 a{color:#2557d6;text-decoration:none} a:hover{text-decoration:underline}
 .wtop{display:flex;align-items:center;gap:18px;background:#f6f7f9;padding:10px 22px}
 .wtop .w{width:34px;height:34px;border-radius:50%;background:#0a2540;color:#f5b700;display:flex;
          align-items:center;justify-content:center;font-weight:800}
 .wsearch{flex:1;max-width:760px;background:#fff;border:1px solid #e1e4ea;border-radius:24px;padding:9px 18px;color:#8a909a}
 .ttl{background:#1e4fc4;color:#fff;padding:16px 24px;font-size:22px;font-weight:700;display:flex;align-items:center;gap:12px}
 .ttl .sub{font-size:14px;font-weight:400;opacity:.92}
 .body{padding:24px 30px;max-width:1500px}
 h2{font-size:18px;margin:22px 0 10px}
 .kv{display:grid;grid-template-columns:200px 1fr;row-gap:12px;max-width:900px}
 .kv .k{font-weight:700} 
 table{width:100%;border-collapse:collapse;margin-top:8px}
 th,td{text-align:left;padding:11px 12px;border:1px solid #e7e9ee;font-size:13px}
 th{background:#f6f8fb;font-weight:700}
 .btn{display:inline-block;border:1px solid #c4ccd6;background:#fff;border-radius:22px;padding:9px 22px;
      cursor:pointer;font-weight:600}
 .btn.pri{background:#2557d6;border-color:#2557d6;color:#fff}
 .bar{height:8px;width:340px;background:#e7e9ee;border-radius:6px;overflow:hidden;display:inline-block;vertical-align:middle}
 .bar > i{display:block;height:100%;background:#2557d6}
 .tabs{display:flex;gap:26px;border-bottom:1px solid #e7e9ee;margin:18px 0}
 .tabs a{padding:12px 2px;color:#5a6472;font-weight:600;border-bottom:3px solid transparent}
 .tabs a.active{color:#2557d6;border-bottom-color:#2557d6}
 .modal-bg{position:fixed;inset:0;background:rgba(20,30,50,.45);display:flex;align-items:flex-start;justify-content:center;z-index:60}
 .modal{background:#fff;border-radius:12px;margin-top:90px;width:720px;max-width:94vw;overflow:hidden}
 .modal .mh{background:#1e4fc4;color:#fff;padding:14px 22px;font-size:18px;font-weight:700}
 .modal .mb{padding:22px}
 .frow{display:grid;grid-template-columns:200px 1fr;align-items:center;margin-bottom:16px}
 .frow .lab{font-weight:700} .frow .lab .req{color:#d33}
 .pill{border:1px solid #c4ccd6;border-radius:6px;padding:8px 10px;display:flex;align-items:center;gap:8px;background:#fff}
 .chip{background:#eef1f5;border-radius:5px;padding:2px 8px}
 .mf{padding:16px 22px;display:flex;gap:12px;border-top:1px solid #eef1f5}
</style></head><body>"""


def ten_nav(search="create int sys"):
    return ('<div class="wtop"><div class="w">W</div>'
            '<div class="wsearch">&#128269;&nbsp; %s</div>'
            '<div style="margin-left:auto;color:#5a6472">&#128172; &#128276; &#128100;</div></div>' % search)


@app.route("/orchestrate/tenant/integration-system/<isid>")
def view_is(isid):
    d = load()
    s = d["integration_systems"].get(isid)
    if not s:
        return Response(TEN_HEAD.replace("__TITLE__", "Not found") + ten_nav() +
                        "<div class='body'>Integration System not found. Deploy an orchestration first.</div></body></html>",
                        mimetype="text/html")
    body = TEN_HEAD.replace("__TITLE__", "Integration System") + ten_nav()
    body += """
    <div class="ttl">View Integration System <span class="sub">__NAME__ &#8230;</span></div>
    <div class="body">
      <h2>Basic Details</h2>
      <div class="kv"><div class="k">System Name</div><div>__NAME__</div></div>
      <div style="margin:14px 0;color:#2557d6">&#9656; System ID &nbsp; <span style="color:#1c1f23">__SID__</span></div>
      <div class="kv" style="max-width:none">
        <div class="k">Integration Template</div><div>Orchestrate Integration Template</div>
        <div class="k">Template Description</div>
        <div>This template is used when implementing an Orchestration. The user must implement the
          External_Integrations WSDL to be invoked by this Template.</div></div>
      <h2>Integration Services <span style="font-weight:400;color:#8a909a">1 item</span></h2>
      <table><tr><th>Integration Template Service</th><th>Initial Service to Invoke</th><th>Optional</th><th>Enabled</th></tr>
        <tr><td>Orchestrate Integration Template / Integration Deployed Orchestrate Service*</td>
          <td>Yes</td><td></td><td>Yes</td></tr></table>
      <h2>Integration Attributes <span style="font-weight:400;color:#8a909a">2 items</span></h2>
      <table><tr><th>Attribute Provider</th><th>Attribute</th><th>Description</th><th>Value</th><th>Restricted to Environment</th></tr>
        <tr><td>Integration Deployed Orchestrate Service</td><td>Orchestration Name</td><td></td><td>__ORCH__</td><td></td></tr>
        <tr><td></td><td>Application Reference ID</td><td></td><td>__APPREF__</td><td></td></tr></table>
      <div style="margin-top:26px">
        <a class="btn pri" href="/orchestrate/tenant/launch/__ISID__">Actions &#8250; Integration &#8250; Launch / Schedule</a>
      </div>
    </div></body></html>"""
    body = (body.replace("__NAME__", esc(s["systemName"])).replace("__SID__", esc(s["systemId"]))
            .replace("__ORCH__", esc(s["orchestrationName"])).replace("__APPREF__", esc(s["appReferenceId"]))
            .replace("__ISID__", isid))
    return Response(body, mimetype="text/html")


@app.route("/orchestrate/tenant/launch/<isid>", methods=["GET", "POST"])
def launch_is(isid):
    d = load()
    s = d["integration_systems"].get(isid)
    if not s:
        return Response("not found", status=404)
    if request.method == "POST":
        # run the deployed orchestration -> create background process + output file
        a = None
        for ap in d["apps"].values():
            if s["orchestrationName"] in ap["orchestrations"]:
                a = ap
                break
        orch = a["orchestrations"][s["orchestrationName"]] if a else {"steps": []}
        # Launch parameters -> Input.* (like Orchestrate Integration Template launch params)
        inputs = {}
        for pname in _launch_param_names(orch):
            v = (request.form.get("lp_" + pname) or "").strip()
            if v:
                inputs[pname] = v
        res = run_orchestration(orch, "Launch", inputs)
        d["counters"]["bp"] += 1
        bp_id = "BP%03d" % d["counters"]["bp"]
        of_ids = []
        for of in res["outputFiles"]:
            d["counters"]["of"] += 1
            ofid = "OF%03d" % d["counters"]["of"]
            d["output_files"][ofid] = {"id": ofid, "title": of["title"], "type": of["type"],
                                       "body": of["body"], "createdBy": "Logan McNeil",
                                       "created": datetime.datetime.now().strftime("%m/%d/%Y %I:%M %p"),
                                       "expires": (datetime.date.today() + datetime.timedelta(days=7)).strftime("%m/%d/%Y")}
            of_ids.append(ofid)
        d["bg_processes"][bp_id] = {
            "id": bp_id, "request": request.form.get("request", s["systemName"]),
            "process": s["systemName"], "status": res["status"], "error": res.get("error"), "isId": isid,
            "system": s["systemName"], "initiatedBy": "Logan McNeil",
            "initiatedAt": datetime.datetime.now().strftime("%m/%d/%Y %I:%M:%S %p"),
            "outputFiles": of_ids, "trace": res["trace"],
        }
        save(d)
        return Response("", status=302, headers={"Location": "/orchestrate/tenant/bg-process/%s" % bp_id})
    body = TEN_HEAD.replace("__TITLE__", "Launch / Schedule Integration") + ten_nav()
    body += """
    <div class="modal-bg" onclick="if(event.target===this)location='/orchestrate/tenant/integration-system/__ISID__'">
      <div class="modal">
        <div class="mh">Launch / Schedule Integration</div>
        <div class="mb">
          <div class="frow"><div class="lab">Integration <span class="req">*</span></div>
            <div class="pill"><span class="chip">&#10005; __NAME__ &#8230;</span></div></div>
          <div class="frow"><div class="lab">Organization</div><div class="pill">&nbsp;</div></div>
          <div class="frow"><div class="lab">Integration System Context</div><div class="pill">&nbsp;</div></div>
          <div class="frow"><div class="lab">Run Frequency <span class="req">*</span></div>
            <div class="pill"><span class="chip">&#10005; Run Now</span></div></div>
          <div style="font-weight:700;margin:14px 0 6px">Launch Parameters</div>
          __LP_ROWS__
        </div>
        <div class="mf">
          <button class="btn pri" onclick="document.getElementById('lf').submit()">OK</button>
          <button class="btn" onclick="location='/orchestrate/tenant/integration-system/__ISID__'">Cancel</button>
        </div>
      </div>
    </div>
    <form id="lf" method="POST" style="display:none"><input name="request" value="__NAME__">__LP_HIDDEN__</form>
    <script>
      document.querySelectorAll("input[data-lp]").forEach(function(i){
        i.oninput=function(){document.getElementById("h_"+i.dataset.lp).value=i.value;};});
    </script>
    </body></html>"""
    a = None
    for ap in d["apps"].values():
        if s["orchestrationName"] in ap["orchestrations"]:
            a = ap
            break
    orch = a["orchestrations"][s["orchestrationName"]] if a else {"steps": []}
    names = _launch_param_names(orch)
    rows = "".join(
        '<div class="frow"><div class="lab">%s</div>'
        '<div class="pill"><input data-lp="%s" placeholder="%s" style="border:0;outline:0;width:100%%;font:inherit"></div></div>'
        % (esc(n), esc(n), esc("21001!21002" if n == "Workers" else ("EmployeeDemographic.csv" if n == "Filename" else "")))
        for n in names) or '<div class="mut">No launch parameters defined.</div>'
    hidden = "".join('<input type="hidden" id="h_%s" name="lp_%s">' % (esc(n), esc(n)) for n in names)
    body = (body.replace("__NAME__", esc(s["systemName"])).replace("__ISID__", isid)
                .replace("__LP_ROWS__", rows).replace("__LP_HIDDEN__", hidden))
    return Response(body, mimetype="text/html")


@app.route("/orchestrate/tenant/bg-process/<bpid>")
def bg_process(bpid):
    d = load()
    bp = d["bg_processes"].get(bpid)
    if not bp:
        return Response("not found", status=404)
    of_rows = ""
    for ofid in bp["outputFiles"]:
        of = d["output_files"][ofid]
        of_rows += ('<tr><td>%s</td><td><a href="/orchestrate/tenant/output/%s">%s</a></td>'
                    '<td>%s</td><td>%s</td><td></td><td>%s</td></tr>'
                    % (of["created"], ofid, esc(of["title"]), of["type"], esc(of["createdBy"]), of["expires"]))
    nof = len(bp["outputFiles"])
    trace_rows = ""
    for e in (bp.get("trace") or []):
        lvl = "ERROR" if e.get("status") == "Error" else "INFO"
        chip = ("background:#fdeaea;color:#c0392b" if lvl == "ERROR" else "background:#e8f1fd;color:#1f6feb")
        if e.get("type") == "LogStep":
            msg = "&lt;LogStep&gt; - %s [Log] [orchId=%s]" % (esc(e["message"]), esc(bp.get("process", "")))
        else:
            msg = "&lt;%s&gt; - %s" % (esc(e.get("type", "Step")), esc(e["message"]))
        trace_rows += ('<tr><td><span style="%s;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">%s</span></td>'
                       '<td style="font-family:Consolas,monospace;font-size:12px">%s</td>'
                       '<td>%s</td></tr>' % (chip, lvl, msg, esc(e.get("step", ""))))
    ntrace = len(bp.get("trace") or [])
    body = TEN_HEAD.replace("__TITLE__", "View Background Process") + ten_nav()
    body += """
    <div class="ttl">View Background Process <span class="sub">__REQ__ &#8230;</span></div>
    <div class="body">
      <div class="kv">
        <div class="k">Process</div><div><a href="#">__PROC__</a></div>
        <div class="k">Request Name</div><div>__REQ__</div>
        <div class="k">Status</div><div>__STATUS__</div>
        <div class="k">Current Processing Time (hh:mm:ss)</div><div>00:00:03</div>
      </div>
      <div class="tabs">
        <a>Integration Details</a><a>Process Info</a><a>Process History</a>
        <a class="active">Output Files (__NOF__)</a><a>Messages (1)</a><a>Child Processes (2)</a></div>
      <h2 style="margin-top:0">Reports and Other Output Files</h2>
      <div style="color:#8a909a;margin-bottom:6px">__NOF__ item</div>
      <table>
        <tr><th>Date and Time Created</th><th>File</th><th>Type</th><th>Created by</th>
            <th>Number of Shared Users</th><th>Expiration Date</th></tr>
        __ROWS__
      </table>
      <h2 style="margin-top:26px">Run Logs <span style="color:#8a909a;font-weight:400">(__NTRACE__)</span></h2>
      <table>
        <tr><th style="width:90px">wd_level</th><th>wd_message</th><th style="width:160px">Step</th></tr>
        __TRACE__
      </table>
    </div></body></html>"""
    body = (body.replace("__REQ__", esc(bp["request"])).replace("__PROC__", esc(bp["process"]))
            .replace("__STATUS__", bp["status"]).replace("__NOF__", str(nof))
            .replace("__NTRACE__", str(ntrace))
            .replace("__TRACE__", trace_rows or "<tr><td colspan=3>No log entries.</td></tr>")
            .replace("__ROWS__", of_rows or "<tr><td colspan=6>No output files.</td></tr>"))
    return Response(body, mimetype="text/html")


@app.route("/orchestrate/tenant/output/<ofid>")
def output_file(ofid):
    d = load()
    of = d["output_files"].get(ofid)
    if not of:
        return Response("not found", status=404)
    return Response(of["body"], mimetype="text/plain",
                    headers={"Content-Disposition": 'attachment; filename="%s.txt"' % of["title"]})


# ===========================================================================
# BUILDER HTML (head + body with the big canvas/palette/properties JS)
# ===========================================================================
BUILDER_HEAD = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>Orchestration Builder</title>
<style>
 *{box-sizing:border-box;margin:0;padding:0}
 body{font-family:'Segoe UI',Roboto,Arial,sans-serif;color:#1c1f23;font-size:14px;background:#fff;overflow:hidden}
 a{color:#2557d6;text-decoration:none}
 /* top bar */
 .top{display:flex;align-items:center;gap:14px;padding:12px 18px;border-bottom:1px solid #eef1f5;height:60px}
 .top .w{width:30px;height:30px;border-radius:50%;background:#0a2540;color:#f5b700;display:flex;
         align-items:center;justify-content:center;font-weight:800}
 .top .crumb{color:#2557d6;font-weight:700;font-size:18px;text-decoration:underline}
 .top .saved{margin-left:auto;color:#5a6472}
 .btn{border:1px solid #c4ccd6;background:#fff;border-radius:22px;padding:8px 18px;cursor:pointer;font-weight:700}
 .btn:hover{background:#f3f6fb}
 .btn.pri{background:#2557d6;border-color:#2557d6;color:#fff}.btn.pri:hover{background:#1f49b8}
 .btn.disabled{opacity:.5;pointer-events:none}
 /* shell */
 .shell{display:flex;height:calc(100vh - 60px - 44px)}
 .rail{width:54px;border-right:1px solid #eef1f5;display:flex;flex-direction:column;align-items:center;
       padding:14px 0;gap:18px;color:#7a828c}
 .rail .r{width:30px;height:30px;display:flex;align-items:center;justify-content:center;border-radius:8px;cursor:pointer}
 .rail .r.act{background:#eef3ff;color:#2557d6}
 .rail .badge{position:relative}
 .rail .badge::after{content:"1";position:absolute;top:-4px;right:-4px;background:#d9480f;color:#fff;
   font-size:10px;border-radius:50%;width:15px;height:15px;display:flex;align-items:center;justify-content:center}
 /* palette */
 .palette{width:300px;border-right:1px solid #eef1f5;display:flex;flex-direction:column}
 .palette .ph{display:flex;align-items:center;padding:16px 18px;font-size:18px;font-weight:700}
 .palette .ph .x{margin-left:auto;color:#5a6472;cursor:pointer;font-size:20px}
 .palette .search{margin:0 14px 10px;border:1px solid #d7dde6;border-radius:8px;padding:10px 12px;color:#8a909a}
 .palette .scroll{overflow:auto;flex:1;padding:0 8px 20px}
 .pal-cat{display:flex;align-items:center;gap:8px;font-weight:700;padding:14px 10px 6px}
 .pal-cat.dr{color:#2f9e44}.pal-cat.do{color:#2563eb}.pal-cat.ol{color:#d9730d}
 .pal-item{display:flex;align-items:center;padding:11px 12px;border-radius:8px;cursor:grab;color:#2b2f36}
 .pal-item:hover{background:#f3f6fb}
 .pal-item .dots{margin-left:auto;color:#b8c0cb;letter-spacing:1px}
 /* canvas */
 .canvas{flex:1;background:radial-gradient(#dfe4ea 1.3px,transparent 1.3px);background-size:20px 20px;
         overflow:auto;position:relative}
 .ctop{position:sticky;top:0;background:#fff;display:flex;align-items:center;gap:10px;padding:14px 18px;
       border-bottom:1px solid #eef1f5;z-index:5}
 .ctop .nm{font-weight:700}.ctop .ic{margin-left:auto;color:#7a828c;font-size:18px;display:flex;gap:14px}
 .flow{display:flex;flex-direction:column;align-items:center;padding:34px 20px 80px}
 .card{width:340px;background:#fff;border:1px solid #e7e9ee;border-radius:12px;box-shadow:0 1px 4px rgba(10,37,64,.06);
       position:relative}
 .card.trigger{padding:24px 24px 18px;text-align:center}
 .card.trigger .tt{font-size:18px;font-weight:700;margin:6px 0}
 .card.trigger .ts{color:#5a6472;font-size:13.5px;line-height:1.4}
 .card.trigger .cfg{display:block;border-top:1px solid #eef1f5;margin-top:16px;padding-top:14px;color:#2557d6;font-weight:700}
 .card.step{padding:22px 16px 18px;text-align:center;cursor:pointer}
 .card.step.sel{border-color:#2557d6;box-shadow:0 0 0 2px #cfe0ff}
 .card .badge{position:absolute;top:-15px;left:50%;transform:translateX(-50%);width:30px;height:30px;border-radius:8px;
              display:flex;align-items:center;justify-content:center}
 .card .ctype{font-weight:800;font-size:13px;letter-spacing:.4px}
 .card.step.dr .ctype{color:#2f9e44}.card.step.do .ctype{color:#2563eb}.card.step.ol .ctype{color:#d9730d}
 .card .cref{color:#5a6472;font-size:13px;margin-top:3px}
 .card .menu{position:absolute;top:10px;right:12px;color:#9aa3b0;cursor:pointer}
 .card.end{padding:22px 16px;text-align:center}
 .card.end .et{font-weight:700;font-size:16px;margin-top:4px}
 .conn{width:2px;height:22px;background:#c4ccd6;position:relative}
 .conn::before,.conn::after{content:"";position:absolute;left:50%;transform:translateX(-50%);
   width:7px;height:7px;border-radius:50%;background:#9aa3b0}
 .conn::before{top:-3px}.conn::after{bottom:-3px}
 .drop{width:340px;border:1.5px dashed #c4ccd6;border-radius:12px;padding:22px;text-align:center;color:#9aa3b0}
 .drop.over{border-color:#2557d6;background:#eef6ff;color:#2557d6}
 .zoom{position:absolute;right:22px;bottom:22px;display:flex;flex-direction:column;gap:0;border:1px solid #e7e9ee;
       border-radius:24px;background:#fff;overflow:hidden}
 .zoom div{width:42px;height:42px;display:flex;align-items:center;justify-content:center;cursor:pointer;font-size:18px;color:#5a6472}
 .zoom div+div{border-top:1px solid #eef1f5}
 /* properties */
 .props{width:520px;border-left:1px solid #eef1f5;display:none;flex-direction:column;overflow:auto}
 .props.show{display:flex}
 .props .ph{display:flex;align-items:center;gap:10px;padding:16px 18px;border-bottom:1px solid #eef1f5}
 .props .ph .pt{font-size:18px;font-weight:700}
 .props .ph .ed{margin-left:auto;background:#eef1f5;border-radius:6px;padding:2px 10px;font-size:12px;color:#5a6472}
 .props .ph .x{color:#5a6472;cursor:pointer;font-size:18px}
 .props .pb{padding:18px;overflow:auto}
 .fld{margin-bottom:16px}
 .fld label{display:block;font-weight:700;margin-bottom:6px}
 .fld label .req{color:#d33}
 .fld input,.fld select{width:100%;border:1px solid #c4ccd6;border-radius:6px;padding:9px 11px;font-size:14px}
 .reqrow{display:flex;gap:0;border:1px solid #c4ccd6;border-radius:8px;overflow:hidden}
 .reqrow select{border:none;border-right:1px solid #e7e9ee;border-radius:0;background:#f6f8fb;width:140px}
 .reqrow .urlp{border:none;border-right:1px solid #e7e9ee;padding:9px 11px;flex:1;color:#1c1f23;background:#fff;outline:none;font-family:inherit;font-size:14px}
 .reqrow .pathin{flex:0 0 210px;border-right:none}
 .reqrow .tokwrap{padding:6px;display:flex;align-items:center;gap:6px;min-width:180px}
 .tabs{display:flex;gap:24px;border-bottom:1px solid #eef1f5;margin:16px 0}
 .tabs a{padding:10px 2px;color:#5a6472;font-weight:700;cursor:pointer;border-bottom:3px solid transparent}
 .tabs a.active{color:#2557d6;border-bottom-color:#2557d6}
 .tokfield{border:1px solid #c4ccd6;border-radius:8px;padding:8px;display:flex;flex-wrap:wrap;gap:6px;align-items:center;position:relative}
 .tok{border-radius:6px;padding:4px 10px;font-size:13px;display:inline-flex;align-items:center;gap:6px}
 .tok.string{background:#23272e;color:#fff}
 .tok.ref{background:#3358cc;color:#fff}
 .tok .rm{cursor:pointer;opacity:.8}
 .addbtn{border:1px solid #d7dde6;background:#f6f8fb;border-radius:6px;width:26px;height:26px;cursor:pointer;color:#5a6472}
 .insbtn{border:1px solid #f0c39a;background:#fdf0e4;border-radius:6px;width:26px;height:26px;cursor:pointer;color:#d9730d}
 .hint{color:#8a909a;font-size:12.5px;margin-top:6px}
 .picker{position:absolute;top:100%;left:0;margin-top:4px;width:420px;background:#fff;border:1px solid #e7e9ee;
         border-radius:10px;box-shadow:0 8px 28px rgba(10,37,64,.16);z-index:30;overflow:hidden}
 .picker .typ{padding:10px 12px;border-bottom:1px solid #eef1f5}
 .picker .typ input{width:100%;border:none;outline:none;font-size:14px}
 .picker .grp{padding:10px 12px;display:flex;align-items:center;gap:8px;cursor:pointer;font-weight:700;color:#1c1f23}
 .picker .grp:hover{background:#f6f8fb}
 .picker .sub{padding:8px 12px 8px 34px;cursor:pointer}.picker .sub:hover{background:#f6f8fb}
 .picker .sub b{color:#2557d6}.picker .sub i{color:#8a909a;font-style:italic;margin-left:8px}
 .picker .add{padding:12px;color:#2557d6;font-weight:700;cursor:pointer;border-top:1px solid #eef1f5}
 .picker .stepname{color:#2f9e44;font-weight:800}
 /* deploy panel */
 .deploy{position:fixed;top:60px;right:0;width:560px;bottom:0;background:#fff;border-left:1px solid #e7e9ee;
         box-shadow:-8px 0 24px rgba(10,37,64,.12);z-index:40;display:none;flex-direction:column}
 .deploy.show{display:flex}
 .deploy .dh{display:flex;align-items:center;gap:10px;padding:18px;border-bottom:1px solid #eef1f5}
 .deploy .dh .ok{color:#1f9d55;font-weight:700}.deploy .dh a{margin-left:auto}
 .deploy .ts{margin:14px 18px;border:1px solid #d7dde6;border-radius:8px;padding:10px 12px;color:#8a909a}
 .deploy .list{overflow:auto;flex:1;padding:0 18px 20px}
 .deploy .trow{display:flex;align-items:center;padding:14px 4px;border-bottom:1px solid #f0f2f6}
 .deploy .trow b{flex:1}
 /* footer */
 .foot{height:44px;border-top:1px solid #eef1f5;display:flex;align-items:center;gap:26px;padding:0 18px}
 .foot a{color:#1c1f23;font-weight:700;cursor:pointer}
 .foot .err{margin-left:auto;display:flex;align-items:center;gap:8px;color:#d9480f;font-weight:700}
 .foot .err .dot{background:#d9480f;color:#fff;border-radius:50%;width:20px;height:20px;display:flex;
   align-items:center;justify-content:center;font-size:12px}
 .console{position:fixed;left:54px;right:0;bottom:44px;max-height:40vh;overflow:auto;background:#0a2540;color:#cfe3ff;
          padding:12px 18px;font-family:Consolas,monospace;font-size:12.5px;display:none;z-index:35}
 .console.show{display:block}
 .console .ok{color:#7fe3a4}.console .er{color:#ff8e8e}.console .mut{color:#7e93ad}
 .console .cx{float:right;cursor:pointer}
 .toast{position:fixed;bottom:60px;left:50%;transform:translateX(-50%);background:#0a2540;color:#fff;
        padding:10px 18px;border-radius:8px;opacity:0;transition:.2s;z-index:80}
 .toast.show{opacity:1}
 /* Create Group + loop container + aggregation + chips */
 .grpbtn{border:1px solid #d7dbe3;background:#fff;border-radius:18px;padding:5px 14px;font-size:13px;font-weight:600;
         color:#1c1f23;cursor:pointer;margin-left:8px}
 .grpbtn:hover{background:#f3f5f8}
 .loopwrap{border:1.5px solid #e3472a;border-radius:14px;background:#fafbfc;padding:6px 16px 14px;
           display:flex;flex-direction:column;align-items:center;min-width:300px}
 .loopwrap.over{background:#fff3f0;border-style:dashed}
 .loopchev{width:30px;height:30px;border-radius:50%;background:#fff;border:1.5px solid #e3472a;color:#e3472a;
           display:flex;align-items:center;justify-content:center;font-weight:800;margin:-20px 0 6px;font-size:13px}
 .loopdrop{border:1.5px dashed #c4ccd6;border-radius:10px;padding:18px;color:#9aa3b0;font-size:13px;width:90%;text-align:center}
 .aggbar{margin-top:2px;background:#eef0f3;border:1px solid #d7dbe3;border-radius:20px;padding:8px 18px;font-weight:800;
         font-size:12.5px;letter-spacing:.4px;color:#3a414d;cursor:pointer;display:flex;align-items:center;gap:10px}
 .aggbar:hover{background:#e6e9ee}
 .aggbar.sel{outline:2px solid #e3472a;outline-offset:1px}
 .aggbar.faint{opacity:.45}
 .aggmenu{cursor:pointer;font-weight:800;color:#5a6472}
 .aggmenupop{position:fixed;background:#fff;border:1px solid #d7dbe3;border-radius:10px;box-shadow:0 8px 26px rgba(0,0,0,.16);
             z-index:90;min-width:210px;padding:6px 0;font-size:14px}
 .aggmenupop .ami{padding:9px 16px;cursor:pointer;color:#1c1f23}
 .aggmenupop .ami:hover{background:#eef6ff;color:#0875e1}
 .chiprow{display:flex;flex-wrap:wrap;align-items:center;gap:6px;border:1px solid #e1e4ea;border-radius:9px;
          padding:8px;background:#fff}
 .chip{border-radius:6px;padding:5px 9px;font-size:12.5px;font-weight:600;border:none;outline:none;font-family:inherit}
 .chip.cref{background:#1f6feb;color:#fff;min-width:120px}
 .chip.citem{background:#1f6feb;color:#fff}
 .chip.cfn{background:#7048d6;color:#fff}
 .chip.cjp{background:#172a3a;color:#9fe6b4;min-width:90px}
 .chip.cbool{background:#0c2a1b;color:#7fe3a4;min-width:90px}
 .sortdir{cursor:pointer;color:#5a6472;font-weight:800;padding:0 4px}
 .chiprm{cursor:pointer;color:#b34;font-weight:700;padding:0 2px}
 .addlink{display:inline-block;margin-top:8px;color:#0875e1;font-weight:600;font-size:13px;cursor:pointer}
 .code{width:100%;min-height:180px;font-family:Consolas,monospace;font-size:12.5px;border:1px solid #d7dbe3;
       border-radius:8px;padding:10px;background:#0a2540;color:#cfe3ff;resize:vertical}
 .aggtbl{width:100%;border-collapse:collapse;margin-top:6px;font-size:13px}
 .aggtbl th{text-align:left;border-bottom:2px solid #e7e9ee;padding:8px 6px;color:#3a414d}
 .aggtbl td{border-bottom:1px solid #eef0f3;padding:6px}
 .aggtbl input,.aggtbl select{width:100%;border:1px solid #d7dbe3;border-radius:6px;padding:6px 8px;font-family:inherit}
 .aggtbl .trash{cursor:pointer}
 .aggopt{display:flex;align-items:center;gap:10px;margin-top:16px;font-weight:600;color:#3a414d}
 .aggempty{text-align:center;color:#9aa3b0;margin-top:36px;line-height:1.5}
 /* ===== Aggregate full editor (video-parity) ===== */
 .aggpanel{position:fixed;left:54px;top:60px;right:0;bottom:44px;background:#fff;z-index:45;display:none;
           flex-direction:column;overflow:auto}
 .aggpanel.show{display:flex}
 .aph{display:flex;align-items:center;gap:10px;padding:16px 22px;border-bottom:1px solid #eef1f5}
 .apic{display:inline-flex;width:28px;height:28px;border-radius:8px;align-items:center;justify-content:center;background:#e3472a}
 .aprow{padding:14px 22px 4px;display:flex;flex-direction:column;gap:6px;font-weight:700;color:#3a414d}
 .apref{display:flex;align-items:center;gap:10px}
 .apref input{flex:1;border:1px solid #c4ccd6;border-radius:8px;padding:10px 12px;font-size:14px;font-family:inherit}
 .apcols{display:grid;grid-template-columns:minmax(340px,44%) 1fr;gap:28px;padding:10px 22px 20px;align-items:start}
 .apadd{color:#3a414d;margin:8px 0 2px;font-size:13.5px}
 .inf{color:#5a6472;cursor:default}
 .aggtbl.big input,.aggtbl.big select{padding:8px}
 .aggtbl tr.selrow td{background:#eef6ff}
 .errn{color:#d9480f;font-weight:700;font-size:12px}
 .apcfg{font-size:14px;color:#1c1f23;margin:8px 0 14px}
 .apright .fld{margin-bottom:16px}
 .apright label{display:block;font-weight:700;color:#3a414d;margin-bottom:6px}
 .apinfo{border-left:4px solid #0875e1;background:#f2f8fe;padding:12px 14px;border-radius:6px;color:#1c1f23;
         font-size:13.5px;margin-top:18px}
 .apfoot{padding:14px 22px;border-top:1px solid #eef1f5}
 .pbox{display:flex;flex-wrap:wrap;align-items:center;gap:6px;border:1.5px solid #c4ccd6;border-radius:8px;
       padding:8px;background:#fff}
 .pbox.errb{border-color:#d9480f}
 .pl{display:inline-flex;align-items:center;gap:5px;border-radius:14px;padding:4px 11px;font-size:12.5px;font-weight:700}
 .pl .prm{cursor:pointer;opacity:0;margin-left:2px;font-size:10px}
 .pl:hover .prm{opacity:.9}
 .pjson{background:#172a3a;color:#fff;border-radius:8px}
 .pfn{background:#7048d6;color:#fff}
 .pref{background:#1f6feb;color:#fff}
 .pagg{background:#e3472a;color:#fff}
 .plit{background:#172a3a;color:#fff}
 .ppar{background:#eef0f3;color:#3a414d;border:1px solid #d7dbe3}
 .pcond{background:#fff;border:1.5px solid #e3472a;color:#e3472a;cursor:pointer}
 .pbox .dots{margin-left:auto;cursor:pointer;color:#5a6472;font-weight:800;padding:0 4px}
 .rawexp{flex:1;border:none;outline:none;font-family:Consolas,monospace;font-size:13px;color:#1c1f23;min-width:200px}
 .experr{color:#d9480f;font-size:12.5px;margin-top:5px;min-height:2px}
 .condmask{position:fixed;inset:0;background:rgba(10,37,64,.45);z-index:70;display:none;
           align-items:flex-start;justify-content:center;overflow:auto}
 .condmask.show{display:flex}
 .conddlg{background:#fff;border-radius:12px;box-shadow:0 18px 60px rgba(0,0,0,.3);width:min(980px,92vw);
          margin:60px 0;padding:26px 30px;position:relative}
 .conddlg h2{margin:0 0 18px;font-size:21px}
 .conddlg .cx{position:absolute;top:16px;right:18px;cursor:pointer;color:#5a6472;font-size:18px}
 .condsec{font-weight:700;color:#3a414d;margin:16px 0 8px}
 .radio{display:flex;align-items:center;gap:10px;margin:8px 0;cursor:pointer;font-size:14px}
 .radio .rd{width:18px;height:18px;border-radius:50%;border:2px solid #98a1ad;display:inline-flex;
            align-items:center;justify-content:center}
 .radio.on .rd{border-color:#0875e1}
 .radio.on .rd::after{content:"";width:9px;height:9px;border-radius:50%;background:#0875e1}
 .condtbl{width:100%;border:1px solid #d7dbe3;border-radius:8px;border-collapse:separate;border-spacing:0;margin-top:6px}
 .condtbl th{text-align:left;padding:10px;border-bottom:1px solid #d7dbe3;color:#3a414d;font-size:13px}
 .condtbl td{padding:10px;border-bottom:1px solid #eef0f3;vertical-align:top}
 .condtbl select{border:1px solid #c4ccd6;border-radius:8px;padding:9px;width:100%;font-family:inherit}
 .savebtn{background:#0875e1;color:#fff;border:none;border-radius:20px;padding:10px 30px;font-weight:700;
          font-size:14px;cursor:pointer;margin-top:20px}
 .pickpop{width:440px;z-index:95}
 .pickpop .noRes{padding:14px;color:#5a6472}
</style></head><body>"""

BUILDER_BODY = r"""
<div class="top">
  <div class="w">W</div>
  <a class="crumb" id="crumb" href="#"></a>
  <span class="saved" id="saved">Saved to session less than a minute ago.</span>
  <button class="btn" id="saveAll">Save All to App Hub</button>
  <button class="btn pri" id="deployBtn" onclick="onDeploy()">Deploy</button>
</div>

<div class="shell">
  <div class="rail">
    <div class="r"><svg viewBox="0 0 24 24" width="20" fill="none" stroke="currentColor" stroke-width="1.7"><rect x="9" y="3" width="6" height="6" rx="1"/><rect x="3" y="15" width="6" height="6" rx="1"/><rect x="15" y="15" width="6" height="6" rx="1"/><path d="M12 9v3M6 15v-1.5h12V15"/></svg></div>
    <div class="r act"><svg viewBox="0 0 24 24" width="20" fill="none" stroke="currentColor" stroke-width="1.7"><path d="M4 7l8-4 8 4-8 4-8-4z"/><path d="M4 12l8 4 8-4M4 17l8 4 8-4"/></svg></div>
    <div class="r"><svg viewBox="0 0 24 24" width="19" fill="none" stroke="currentColor" stroke-width="1.7"><circle cx="11" cy="11" r="7"/><path d="M21 21l-4-4"/></svg></div>
    <div style="flex:1"></div>
    <div class="r">&#9000;</div>
    <div class="r badge">&#128227;</div>
    <div class="r"><i>fx</i></div>
    <div class="r">&#128295;</div>
  </div>

  <div class="palette">
    <div class="ph">Components <span class="x" title="(visual)">&#10005;</span></div>
    <div class="search">&#128269;&nbsp; Search Components</div>
    <div class="scroll" id="palette"></div>
  </div>

  <div class="canvas" id="canvasWrap">
    <div class="ctop"><span class="nm" id="orchName"></span> <span style="color:#9aa3b0">&#8942;</span>
      <button class="grpbtn" onclick="alert('Create Group groups selected steps (visual in the mock).')">Create Group</button>
      <span class="ic">&#9889; &#8635; &#9881;</span></div>
    <div class="flow" id="flow"></div>
    <div class="zoom"><div onclick="zoom(0)">&#8635;</div><div onclick="zoom(1)">+</div><div onclick="zoom(-1)">&#8722;</div></div>
  </div>

  <div class="props" id="props">
    <div class="ph"><span id="pIcon"></span><span class="pt" id="pType"></span>
      <span class="ed">Editing</span><span style="color:#9aa3b0">&#128221;</span>
      <span style="color:#9aa3b0">&#8689;</span><span class="x" onclick="deselect()">&#10005;</span></div>
    <div class="pb" id="pBody"></div>
  </div>
</div>

<div class="foot">
  <a onclick="toggleConsole('build')">Build Logs</a>
  <a onclick="toggleConsole('run')">Run Logs</a>
  <div class="err" id="errBadge" style="display:none"><span class="dot">!</span><span id="errCount">1</span></div>
  <button class="btn" style="margin-left:auto" onclick="validate()">Validate</button>
</div>

<div class="deploy" id="deploy">
  <div class="dh"><span class="ok">&#10004; Build #<span id="bn"></span> succeeded</span><a onclick="toggleConsole('build')">View Build Logs</a></div>
  <div class="ts">&#128269;&nbsp; Search tenants</div>
  <div class="list" id="tenantList"></div>
</div>

<div class="console" id="console"><span class="cx" onclick="document.getElementById('console').classList.remove('show')">&#10005;</span><div id="consoleBody"></div></div>
<div class="aggpanel" id="aggPanel"></div>
<div class="condmask" id="condModal"></div>
<div class="toast" id="toast"></div>

<script>
const DATA = __DATA__;
const ORCH = DATA.orch;
const APPID = DATA.appId;
let sel=null, selKind="step", scale=1, builtNo=DATA.buildNo-1, isBuilt=false;

const SVG_DB='<svg viewBox="0 0 24 24" width="15" height="15" fill="#fff"><ellipse cx="12" cy="5" rx="8" ry="3"/><path d="M4 5v6c0 1.7 3.6 3 8 3s8-1.3 8-3V5"/><path d="M4 11v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"/></svg>';
const SVG_BR='<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="#fff" stroke-width="2.2"><path d="M9 4H6v16h3M15 4h3v16h-3"/></svg>';
const SVG_BRANCH='<svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="#fff" stroke-width="2"><path d="M6 3v6M6 9a6 6 0 0 0 6 6h6M18 11l3-2-3-2M18 19l3-2-3-2"/></svg>';
const PLUG_SVG='<svg viewBox="0 0 24 24" width="40" height="40" fill="none" stroke="#5a6472" stroke-width="1.5"><path d="M8 3v5M16 3v5M6 8h12v3a6 6 0 0 1-12 0V8z"/><path d="M12 17v4"/></svg>';
const MEGA_SVG='<svg viewBox="0 0 24 24" width="40" height="40" fill="none" stroke="#5a6472" stroke-width="1.5"><path d="M3 11v2a1 1 0 0 0 1 1h2l9 5V6L6 11H4a1 1 0 0 0-1 0z"/><path d="M18 9a3 3 0 0 1 0 6"/></svg>';
const RABBIT_SVG='<svg viewBox="0 0 24 24" width="44" height="44" fill="none" stroke="#5a6472" stroke-width="1.4"><path d="M3 17c3 1 7 1 10-1 2-1.3 3-3.5 6-3.5 1.7 0 2.6 1 2.6 2s-1 1.8-2 1.8"/><path d="M7 15c-2 0-3.6 1.2-3.6 3"/><path d="M14 9c-.4-2.6.6-4.6 2.6-5.6M16.6 9.2c.6-2.6 2.4-3.8 4.4-3.8"/></svg>';
const BP_SVG='<svg viewBox="0 0 24 24" width="42" height="42" fill="none" stroke="#5a6472" stroke-width="1.4"><circle cx="12" cy="4" r="2"/><circle cx="4" cy="12" r="2"/><circle cx="20" cy="12" r="2"/><circle cx="12" cy="20" r="2"/><path d="M12 6v2M12 16v2M6 12h2M16 12h2M7.5 7.5l2 2M14.5 14.5l2 2M16.5 7.5l-2 2M9.5 14.5l-2 2"/></svg>';
const PLANE_SVG='<svg viewBox="0 0 24 24" width="40" height="40" fill="none" stroke="#5a6472" stroke-width="1.4"><path d="M22 2 11 13M22 2l-7 20-4-9-9-4 20-7z"/></svg>';

// component catalog (exact names + categories)
const CAT = {
 "Data Requests": {cls:"dr", color:"#2f9e44", icon:SVG_DB, items:[
   ["Send Paged HTTP Request","send-paged-http-request"],
   ["Send HTTP Request","send-http-request"],
   ["Send Prism Request","send-prism-request"],
   ["Send Workday API Request","send-workday-api-request"],
   ["Send Workday RaaS Request","send-workday-raas-request"],
   ["Send Paged Workday REST Call","send-paged-workday-rest-call"],
   ["Send Paged Workday SOAP Call","send-paged-workday-soap-call"],
   ["Trigger Business Process","trigger-business-process"],
   ["Trigger Integration","trigger-integration"],
   ["Trigger PDF Generation","trigger-pdf-generation"]]},
 "Data Operations": {cls:"do", color:"#2563eb", icon:SVG_BR, items:[
   ["Create JSON","create-json"],
   ["Create Text Template","create-text-template"],
   ["Create Values","create-values"],
   ["Store Document","store-document"],
   ["Validate","validate"]]},
 "Orchestration Logic": {cls:"ol", color:"#e3472a", icon:SVG_BRANCH, items:[
   ["Batch Loop","batch-loop"],
   ["Branch on Conditions","branch-on-conditions"],
   ["Continue on Conditions","continue-on-conditions"],
   ["Send Integration Message","send-integration-message"],
   ["Join Loop","join-loop"],
   ["Log","log"],
   ["Loop","loop"]]},
 "Amazon Web Services (AWS)": {cls:"aws", color:"#d9730d", icon:SVG_BRANCH, items:[
   ["Put Amazon EventBridge Event","put-amazon-eventbridge-event"],
   ["Invoke AWS Lambda Function","invoke-aws-lambda-function"]]}
};
function typeMeta(type){
  for(const c in CAT){for(const it of CAT[c].items){if(it[1]===type)return {name:it[0],cls:CAT[c].cls,color:CAT[c].color,icon:CAT[c].icon};}}
  return {name:type,cls:"do",color:"#2563eb",icon:SVG_BR};
}
function defaultRef(type){return typeMeta(type).name.replace(/[^A-Za-z]/g,"");}

let dragType=null;
function buildPalette(){
  let h="";
  for(const c in CAT){
    const cat=CAT[c];
    h+="<div class='pal-cat "+cat.cls+"'>"+cat.icon.replace(/#fff/g,cat.color)+" "+c+"</div>";
    for(const it of cat.items){
      h+="<div class='pal-item' draggable='true' ondragstart='dragType=\""+it[1]+"\"' onclick='addStep(\""+it[1]+"\")'>"
        +it[0]+"<span class='dots'>&#10303;</span></div>";
    }
  }
  document.getElementById("palette").innerHTML=h;
}

function newStep(type){
  const s={id:"n"+Math.random().toString(36).slice(2,7),type:type,ref:defaultRef(type),props:defaults(type)};
  if(["loop","batch-loop","join-loop","branch-on-conditions","continue-on-conditions"].includes(type)) s.body=[];
  if(["loop","batch-loop"].includes(type)) s.aggregation={ref:"Aggregate",outputs:[],failWhenNoInputs:false,earlyStop:null,errorHandler:false,deleted:false};
  return s;
}
function defaults(type){
  if(type==="send-workday-raas-request")return{method:"GET",urlPrefix:"http://localhost:8443/task/view-report?name=",
    path:"",auth:"Default Workday API Credential",contentType:"Any",queryParams:[]};
  if(type==="store-document")return{documentToStore:[],documentTitle:[],description:[],collection:[],
    expiresIn:"7",expiresUnit:"Days",attachToEvent:"true",deliver:"false"};
  if(type==="create-text-template")return{contentType:"application/json",message:""};
  if(type==="loop"||type==="batch-loop"||type==="join-loop")
    return{dataType:"AutoType Iterator",dataSetRef:"",dataSetPath:"",filterPath:"",sortBy:[],locale:""};
  if(type==="log")return{messageRef:"",condition:"true"};
  if(type==="continue-on-conditions")return{assertions:[{expr:"",message:""}]};
  if(type==="send-integration-message")return{severity:"INFO",summary:""};
  if(type==="create-json"||type==="create-values")return{values:[]};
  if(type==="validate")return{};
  if(type.startsWith("put-amazon")||type.startsWith("invoke-aws"))return{};
  if(type==="send-http-request")return{method:"GET",url:"",auth:"No Auth",advancedMode:false,queryParams:[]};
  if(type.startsWith("send-")||type.startsWith("trigger-"))return{method:"GET",url:"",auth:"Default Workday API Credential"};
  return{};
}
function addStep(type){ORCH.steps.push(newStep(type));render();save(true);}
function addToLoop(loopId,type){const l=findStep(ORCH.steps,loopId);if(!l)return;l.body=l.body||[];l.body.push(newStep(type));render();save(true);}
function findStep(steps,id){for(const s of (steps||[])){if(s.id===id)return s;if(s.body){const r=findStep(s.body,id);if(r)return r;}}return null;}
function removeStep(id){
  function rm(steps){const i=steps.findIndex(x=>x.id===id);if(i>=0){steps.splice(i,1);return true;}for(const x of steps){if(x.body&&rm(x.body))return true;}return false;}
  rm(ORCH.steps);if(sel&&sel.id===id)deselect();render();save(true);
}

/* ---- canvas ---- */
function render(){
  document.getElementById("crumb").textContent=DATA.appName;
  document.getElementById("orchName").textContent=ORCH.name;
  const sync=(ORCH.startType==="synchronous"||ORCH.startType==="asynchronous");
  const bp=(ORCH.startType==="business-process");
  let h="";
  if(bp){
    h+="<div class='card trigger'>"+BP_SVG+
       "<div class='tt'>Business Process Trigger</div>"+
       "<div class='ts'>Orchestration will listen for a business process request</div>"+
       "<a class='cfg'>Configure Request &#8594;</a></div>";
  } else if(sync){
    h+="<div class='card trigger'>"+RABBIT_SVG+
       "<div class='tt'>Orchestration Start</div>"+
       "<div class='ts'>Synchronous orchestration will immediately return an HTTP response</div>"+
       "<a class='cfg'>Configure Request &#8594;</a></div>";
  } else {
    h+="<div class='card trigger'>"+PLUG_SVG+
       "<div class='tt'>Integration Framework Trigger</div>"+
       "<div class='ts'>Orchestration will be triggered by a Workday integration system</div>"+
       "<a class='cfg'>Configure Parameters &#8594;</a></div>";
  }
  ORCH.steps.forEach(s=>{ h+="<div class='conn'></div>"+renderStep(s); });
  h+="<div class='conn'></div>";
  if(bp){
    h+="<div class='card end'>"+MEGA_SVG+
       "<div class='et'>Orchestration End</div>"+
       "<div class='ts'>Business process will respond based on its own configuration</div></div>";
  } else if(sync){
    h+="<div class='card end'>"+PLANE_SVG+
       "<div class='et'>Orchestration End</div>"+
       "<div class='ts'>Setup response format to share with requesting service</div>"+
       "<a class='cfg'>Configure Response &#8594;</a></div>";
  } else {
    h+="<div class='card end'>"+MEGA_SVG+"<div class='et'>Orchestration End</div></div>";
  }
  const flow=document.getElementById("flow");
  flow.innerHTML=h; flow.style.transform="scale("+scale+")"; flow.style.transformOrigin="top center";
  attachLoopDnD();
}
function renderStep(s){
  const m=typeMeta(s.type);
  const isLoop=["loop","batch-loop","join-loop"].includes(s.type);
  let h="<div class='card step "+m.cls+(selKind==='step'&&sel&&sel.id===s.id?" sel":"")+"' onclick='select(\""+s.id+"\")'>"+
     "<span class='menu' onclick='event.stopPropagation();removeStep(\""+s.id+"\")'>&#8942;</span>"+
     "<span class='badge' style='background:"+m.color+"'>"+m.icon+"</span>"+
     "<div class='ctype'>"+m.name.toUpperCase()+"</div>"+
     "<div class='cref'>"+esc(s.ref)+"</div></div>";
  if(isLoop){
    h+="<div class='conn'></div>";
    h+="<div class='loopwrap' data-loop='"+s.id+"'>";
    h+="<div class='loopchev'>&#94;</div>";
    const body=s.body||[];
    body.forEach((c,i)=>{ if(i)h+="<div class='conn'></div>"; h+=renderStep(c); });
    if(!body.length) h+="<div class='loopdrop'>Drop a step here to run it inside the loop</div>";
    const agg=s.aggregation;
    if(agg){
      const faint=agg.deleted?" faint":"";
      h+="<div class='conn'></div><div class='aggbar"+faint+(selKind==='aggregate'&&sel&&sel.id===s.id?" sel":"")+"' onclick='enableAgg(\""+s.id+"\")'>CONFIGURE AGGREGATION"+
         "<span class='aggmenu' onclick='event.stopPropagation();toggleAggMenu(event,\""+s.id+"\")'>&#8943;</span></div>";
    }
    h+="</div>";
  }
  return h;
}
function esc(t){return (t||"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");}
function jv(t){return esc(t).replace(/'/g,"&#39;");}
function pathStr(p){return Array.isArray(p)? p.map(t=>t.v).join("") : (p||"");}
function zoom(d){if(d===0)scale=1;else scale=Math.max(.5,Math.min(1.6,scale+d*.1));render();}

/* drag onto canvas */
document.getElementById("canvasWrap").addEventListener("dragover",e=>e.preventDefault());
document.getElementById("canvasWrap").addEventListener("drop",e=>{e.preventDefault();if(dragType){addStep(dragType);dragType=null;}});

/* ---- select + properties ---- */
function select(id){sel=findStep(ORCH.steps,id);selKind="step";closeAggMenu();render();renderProps();}
function selectAgg(loopId){sel=findStep(ORCH.steps,loopId);selKind="aggregate";closeAggMenu();render();renderProps();}
function enableAgg(id){const l=findStep(ORCH.steps,id);if(l&&l.aggregation)l.aggregation.deleted=false;selectAgg(id);save(true);}
function deselect(){sel=null;selKind="step";document.getElementById("props").classList.remove("show");
  const ap=document.getElementById("aggPanel");if(ap)ap.classList.remove("show");render();}
function setRef(v){sel.ref=v;render();}
function setp(k,v){sel.props[k]=v;}

function renderProps(){
  if(!sel)return;
  if(selKind==="aggregate"){renderAggProps();return;}
  const m=typeMeta(sel.type);
  document.getElementById("pIcon").innerHTML="<span style='display:inline-flex;width:26px;height:26px;border-radius:7px;align-items:center;justify-content:center;background:"+m.color+"'>"+m.icon+"</span>";
  document.getElementById("pType").textContent=m.name;
  let h="<div class='fld'><label>Reference Name <span class='req'>*</span></label>"+
        "<input value='"+esc(sel.ref)+"' oninput='setRef(this.value)' onchange='save(true)'></div>";
  if(sel.type==="send-workday-raas-request"){
    sel.props.path = pathStr(sel.props.path);
    h+="<div class='reqrow'>"+
       "<select onchange='setp(\"method\",this.value);save(true)'>"+opts(['GET','POST','PUT','DELETE'],sel.props.method)+"</select>"+
       "<input class='urlp' value='"+jv(sel.props.urlPrefix)+"' placeholder='http://localhost:8443/task/view-report?name=' "+
       "oninput='setp(\"urlPrefix\",this.value)' onchange='save(true)'>"+
       "<input class='urlp pathin' value='"+jv(sel.props.path)+"' placeholder='report name' "+
       "oninput='setp(\"path\",this.value)' onchange='save(true)'></div>";
    h+="<div class='tabs'><a class='active'>General</a><a>Headers</a><a>Query Parameters</a><a>Settings</a></div>";
    h+="<p class='hint'>Both fields are editable (no hardcoding). Base URL + report name, e.g. "+
       "<code>http://localhost:8443/task/view-report?name=</code> + <code>CRT_INT01_Raas</code>. "+
       "<b>Run</b> / <b>Launch</b> fetches that URL live and feeds the result into the next step.</p>";
    h+="<p class='hint'>Select a saved credential. To create a new authentication credential, <a href='#'>open settings</a>.</p>";
    h+="<div class='fld'><label>Authentication</label><select onchange='setp(\"auth\",this.value);save(true)'>"+
       opts(['Default Workday API Credential','Access Token from Initiating User'],sel.props.auth)+"</select></div>";
    h+="<div class='fld'><label>Content Type</label><select onchange='setp(\"contentType\",this.value);save(true)'>"+
       opts(['Any','application/json','text/xml'],sel.props.contentType)+"</select></div>";
    sel.props.queryParams=sel.props.queryParams||[];
    h+="<div style='margin-top:18px;font-weight:700'>Query Parameters (report prompts)</div>";
    h+="<p class='hint'>e.g. <code>Workers!Employee_ID</code> = <code>21001!21002</code>. Use <code>{Input.Workers}</code> to pull a launch parameter. Blank values are not sent.</p>";
    h+="<table class='aggtbl'><tr><th>Parameter</th><th>Value</th><th></th></tr>";
    sel.props.queryParams.forEach(function(q,i){
      h+="<tr><td><input value='"+jv(q.key)+"' placeholder='Workers!Employee_ID' oninput='setQP("+i+",\"key\",this.value)' onchange='save(true)'></td>"+
         "<td><input value='"+jv(q.value)+"' placeholder='Enter a value' oninput='setQP("+i+",\"value\",this.value)' onchange='save(true)'></td>"+
         "<td style='text-align:center'><span class='trash' onclick='rmQP("+i+")'>&#128465;</span></td></tr>";
    });
    h+="</table><a class='addlink' onclick='addQP()'>+ Add Parameter</a>";
  } else if(sel.type==="continue-on-conditions"){
    sel.props.assertions=sel.props.assertions||[];
    h+="<p class='hint'><b>Assertions</b> must all be true for the orchestration to continue. "+
       "If any is false the run fails with its <b>Message</b>. Supported: <code>.is2xxStatusCode()</code>, "+
       "<code>.countItemsAtJsonPath(\"$.Report_Entry\").greaterThan(0)</code>, <code>.isBlank()</code>, "+
       "<code>.length().lessThanOrEquals(2000)</code>, <code>.or(...)</code>, <code>==</code>, <code>&gt;</code>.</p>";
    h+="<table class='aggtbl'><tr><th>Assertion (expression)</th><th>Message</th><th></th></tr>";
    sel.props.assertions.forEach(function(a,i){
      h+="<tr><td><input value='"+jv(a.expr)+"' placeholder='data.SendWorkdayRaaSRequest.responseStatusCode.is2xxStatusCode()' oninput='setAS("+i+",\"expr\",this.value)' onchange='save(true)'></td>"+
         "<td><input value='"+jv(a.message)+"' placeholder='Report returned no data' oninput='setAS("+i+",\"message\",this.value)' onchange='save(true)'></td>"+
         "<td style='text-align:center'><span class='trash' onclick='rmAS("+i+")'>&#128465;</span></td></tr>";
    });
    h+="</table><a class='addlink' onclick='addAS()'>+ Add Assertion</a>";
  } else if(sel.type==="send-integration-message"){
    h+="<div class='fld'><label>Severity <span class='req'>*</span></label><select onchange='setp(\"severity\",this.value);save(true)'>"+
       opts(['INFO','WARN','ERROR'],sel.props.severity||'INFO')+"</select></div>";
    h+="<div class='fld'><label>Summary <span class='req'>*</span></label>"+
       "<input value='"+jv(sel.props.summary||'')+"' placeholder='Integration started.' oninput='setp(\"summary\",this.value)' onchange='save(true)'></div>";
    h+="<p class='hint'>Writes a message to the integration event (Background Process). <code>{Step.path}</code> tokens are substituted.</p>";
  } else if(sel.type==="store-document"){
    h+="<div class='tabs'><a class='active'>General</a><a>Settings</a></div>";
    h+=tokField("Document to Store","documentToStore",true);
    h+=tokField("Document Title","documentTitle",true);
    h+=tokField("Description","description",false);
    h+=tokField("Collection to Store Document In","collection",false);
    h+="<p class='hint'>Collection will default to Integration System WID if not overwritten.</p>";
    h+="<div class='fld'><label>Storage Expires In</label><div style='display:flex;gap:8px'>"+
       "<input style='width:90px' value='"+esc(sel.props.expiresIn)+"' oninput='setp(\"expiresIn\",this.value)'>"+
       "<select onchange='setp(\"expiresUnit\",this.value)'>"+opts(['Days','Hours','Weeks'],sel.props.expiresUnit)+"</select></div></div>";
    h+="<div class='fld'><label>Attach document to integration event <span class='req'>*</span></label>"+
       "<select onchange='setp(\"attachToEvent\",this.value)'>"+opts(['true','false'],sel.props.attachToEvent)+"</select></div>";
    h+="<div class='fld'><label>Deliver from associated delivery services <span class='req'>*</span></label>"+
       "<select onchange='setp(\"deliver\",this.value)'>"+opts(['true','false'],sel.props.deliver)+"</select></div>";
  } else if(sel.type==="create-text-template"){
    h+="<div class='fld'><label>Content Type <span class='req'>*</span></label>"+
       "<input value='"+jv(sel.props.contentType||'')+"' oninput='setp(\"contentType\",this.value)' onchange='save(true)'></div>";
    h+="<div class='fld'><label>Message</label><textarea class='code' spellcheck='false' oninput='setp(\"message\",this.value)' onchange='save(true)'>"+esc(sel.props.message||'')+"</textarea></div>";
  } else if(["loop","batch-loop","join-loop"].includes(sel.type)){
    h+=loopPropsHtml();
  } else if(sel.type==="log"){
    h+=logPropsHtml();
  } else if(sel.type==="send-http-request"){
    h+="<div class='fld'><label style='display:inline-flex;align-items:center;gap:8px;font-weight:600'>"+
       "<input type='checkbox' "+(sel.props.advancedMode?'checked':'')+" onchange='setp(\"advancedMode\",this.checked);save(true)'> Advanced Mode</label></div>";
    h+="<div class='reqrow'>"+
       "<select onchange='setp(\"method\",this.value);save(true)'>"+opts(['GET','POST','PUT','DELETE','PATCH'],sel.props.method||'GET')+"</select>"+
       "<input class='urlp' value='"+esc(sel.props.url||'')+"' placeholder='https://api.example.com/v3/...' "+
       "oninput='setp(\"url\",this.value)' onchange='save(true)'></div>";
    h+="<div class='tabs'><a class='active'>Auth</a><a>Body</a><a>Headers</a><a>Query Parameters</a><a>Settings</a></div>";
    h+="<p class='hint'><b>Specify Authentication.</b> Pick a saved credential, or <b>No Auth</b> for public APIs. "+
       "<b>Run</b> fires a real GET to this URL and feeds the JSON into the next step.</p>";
    h+="<div class='fld'><label>Authentication</label><select onchange='setp(\"auth\",this.value);save(true)'>"+
       opts(['No Auth','Default Workday API Credential','Access Token from Initiating User'],sel.props.auth||'No Auth')+"</select></div>";
    sel.props.queryParams=sel.props.queryParams||[];
    h+="<div style='margin-top:18px;font-weight:700'>Query Parameters</div>";
    h+="<p class='hint'>Key/Value pairs appended to the URL after <code>?</code> and separated by <code>&amp;</code>.</p>";
    h+="<table class='aggtbl'><tr><th>Parameter</th><th>Value</th><th></th></tr>";
    sel.props.queryParams.forEach(function(q,i){
      h+="<tr><td><input value='"+jv(q.key)+"' placeholder='apiKey' oninput='setQP("+i+",\"key\",this.value)' onchange='save(true)'></td>"+
         "<td><input value='"+jv(q.value)+"' placeholder='Enter a value' oninput='setQP("+i+",\"value\",this.value)' onchange='save(true)'></td>"+
         "<td style='text-align:center'><span class='trash' onclick='rmQP("+i+")'>&#128465;</span></td></tr>";
    });
    h+="</table><a class='addlink' onclick='addQP()'>+ Add Parameter</a>";
    h+="<p class='hint'>Self-contained demo endpoint: <code>http://localhost:8443/orchestrate/mock-api/stock?ticker=AAPL</code>. "+
       "Swap in a real API (Polygon, Alpha Vantage) anytime.</p>";
  } else if(sel.type==="create-values"||sel.type==="create-json"){
    sel.props.values=sel.props.values||[];
    h+="<p class='hint'>Extract data points from a previous step using JSON path. "+
       "e.g. <code>SendHTTPRequest.response</code> + <code>$.results[0].o</code> for the daily open price.</p>";
    h+="<table class='aggtbl'><tr><th>Name</th><th>Source (Step.response)</th><th>JSON Path</th><th></th></tr>";
    sel.props.values.forEach(function(v,i){
      h+="<tr><td><input value='"+jv(v.name)+"' oninput='setCV("+i+",\"name\",this.value)' onchange='save(true)'></td>"+
         "<td><input value='"+jv(v.sourceRef)+"' placeholder='SendHTTPRequest.response' oninput='setCV("+i+",\"sourceRef\",this.value)' onchange='save(true)'></td>"+
         "<td><input value='"+jv(v.jsonPath)+"' placeholder='$.results[0].o' oninput='setCV("+i+",\"jsonPath\",this.value)' onchange='save(true)'></td>"+
         "<td style='text-align:center'><span class='trash' onclick='rmCV("+i+")'>&#128465;</span></td></tr>";
    });
    h+="</table><a class='addlink' onclick='addCV()'>+ Add Value</a>";
  } else {
    h+="<div class='reqrow'><select onchange='setp(\"method\",this.value)'>"+opts(['GET','POST','PUT','DELETE'],sel.props.method||'GET')+"</select>"+
       "<input class='urlp' value='"+esc(sel.props.url||'')+"' placeholder='https://...' oninput='setp(\"url\",this.value)' onchange='save(true)'></div>";
    h+="<div class='fld' style='margin-top:14px'><label>Authentication</label><select onchange='setp(\"auth\",this.value)'>"+
       opts(['Default Workday API Credential','Access Token from Initiating User'],sel.props.auth||'Default Workday API Credential')+"</select></div>";
  }
  const pb=document.getElementById("pBody");pb.innerHTML=h;
  document.getElementById("props").classList.add("show");
  if(sel.type==="store-document"){["documentToStore","documentTitle","description","collection"].forEach(k=>tfRefresh(k));}
  document.querySelectorAll(".tabs a").forEach(t=>t.onclick=function(){
    this.parentNode.querySelectorAll("a").forEach(x=>x.classList.remove("active"));this.classList.add("active");});
}
function opts(arr,cur){return arr.map(o=>"<option"+(o===cur?" selected":"")+">"+o+"</option>").join("");}

/* ---- token fields + expression builder picker ---- */
function tokField(label,key,req){
  return "<div class='fld'><label>"+label+(req?" <span class='req'>*</span>":"")+"</label>"+
         "<div class='tokfield' id='tf_"+key+"'></div></div>";
}
function renderTok(elId,key,inline){
  const el=document.getElementById(elId)||document.getElementById("tf_"+key);
  if(!el)return;
  const toks=sel.props[key]||[];
  let h="";
  toks.forEach((t,i)=>{h+="<span class='tok "+(t.t==='ref'?'ref':'string')+"'>"+esc(t.v)+
    " <span class='rm' onclick='rmTok(\""+key+"\","+i+")'>&#10005;</span></span>";});
  h+="<button class='addbtn' onclick='openPicker(event,\""+key+"\")'>+</button>";
  if(!inline)h+="<button class='insbtn' onclick='openPicker(event,\""+key+"\",true)'>&#8599;</button>";
  el.innerHTML=h;
}
function tfRefresh(key){renderTok("tf_"+key,key,false);}
function rmTok(key,i){sel.props[key].splice(i,1);(key==='path'?renderTok("tw_path",key,true):tfRefresh(key));}
function openPicker(ev,key,refOnly){
  ev.stopPropagation();
  closePicker();
  const wrap=ev.target.closest(".tokfield")||ev.target.closest(".tokwrap");
  const steps=ORCH.steps.filter(s=>s.id!==(sel?sel.id:''));
  let subs="";
  launchParamNames().forEach(n=>{const p={name:n};
    if(p&&p.name)subs+="<div class='sub' onclick='pickRef(\""+key+"\",\"Input."+jv(p.name)+"\")'><b>Input."+esc(p.name)+"</b><i>Launch parameter</i></div>";
  });
  steps.forEach(s=>{
    const m=typeMeta(s.type);
    subs+="<div class='grp'>&#9660; <span class='stepname' style='color:"+m.color+"'>"+m.name.toUpperCase()+"</span> "+esc(s.ref)+"</div>";
    if(s.aggregation&&!s.aggregation.deleted&&(s.aggregation.outputs||[]).length){
      const ar=s.aggregation.ref||"Aggregate";
      s.aggregation.outputs.forEach(o=>{
        subs+="<div class='sub' onclick='pickRef(\""+key+"\",\""+jv(ar)+"."+jv(o.name)+"\")'><b>"+esc(ar)+"."+esc(o.name)+"</b><i>"+esc(o.strategy||"")+" output</i></div>";
      });
    } else if(s.type==="create-values"||s.type==="create-json"){
      (s.props&&s.props.values||[]).forEach(v=>{
        subs+="<div class='sub' onclick='pickRef(\""+key+"\",\""+jv(s.ref)+"."+jv(v.name)+"\")'><b>"+esc(v.name)+"</b><i>Value</i></div>";
      });
    } else {
      subs+="<div class='sub' onclick='pickRef(\""+key+"\",\""+s.ref+".response\")'><b>response</b><i>Data</i></div>";
      subs+="<div class='sub' onclick='pickRef(\""+key+"\",\""+s.ref+".responseHeaders\")'><b>responseHeaders</b><i>Headers</i></div>";
      subs+="<div class='sub' onclick='pickRef(\""+key+"\",\""+s.ref+".responseStatusCode\")'><b>responseStatusCode</b><i>Number</i></div>";
    }
  });
  const p=document.createElement("div");p.className="picker";p.id="picker";
  p.innerHTML="<div class='typ'><input id='pickType' placeholder='Type here' oninput='pickTyping(\""+key+"\",this.value)'></div>"+
    "<div id='pickResults'><div class='grp'>{ } Data from Orchestration Steps</div>"+subs+
    "<div class='grp'><i>fx</i> Global Functions <span style='margin-left:auto'>&#8250;</span></div></div>"+
    "<div class='add'>&#129518; Explore All Functions</div>";
  wrap.style.position="relative";wrap.appendChild(p);
  setTimeout(()=>document.getElementById("pickType").focus(),0);
}
function pickTyping(key,val){
  const r=document.getElementById("pickResults");if(!r)return;
  const v=val.trim();
  if(v){
    const clean=v.replace(/\\/g,"").replace(/\"/g,"");
    let h="";
    // dotted path (Aggregate.outputFile, Input.Filename, Step.response) -> reference pill
    if(/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z0-9_\[\]\$\*]+)+$/.test(v))
      h+="<div class='add' onclick='pickRef(\""+key+"\",\""+clean+"\")'>+ <b>Add reference</b> <span class='stepname'>"+esc(v)+"</span></div>";
    h+="<div class='add' onclick='pickStrTok(\""+key+"\",\""+clean+"\")'>+ Add string \""+esc(v)+"\"</div>";
    // matching known refs
    const ql=v.toLowerCase();
    stepRefs().filter(f=>f.toLowerCase().indexOf(ql)>=0)
      .forEach(rf=>h+="<div class='sub' onclick='pickRef(\""+key+"\",\""+jv(rf)+"\")'><span class='stepname'>"+esc(rf)+"</span><i>data reference</i></div>");
    r.innerHTML=h;
  }
}
function launchParamNames(){
  const out=[];(ORCH.launchParams||[]).forEach(p=>{if(p&&p.name&&out.indexOf(p.name)<0)out.push(p.name);});
  (JSON.stringify(ORCH.steps||[]).match(/Input\.[A-Za-z0-9_]+/g)||[]).forEach(m=>{const n=m.slice(6);if(out.indexOf(n)<0)out.push(n);});
  ["Workers","Filename"].forEach(n=>{if(out.indexOf(n)<0)out.push(n);});
  return out;
}
/* every data reference a step-props picker can offer: Input.*, Step.response, Aggregate.<output> */
function stepRefs(){
  const out=[];
  launchParamNames().forEach(n=>out.push("Input."+n));
  function walk(steps){
    (steps||[]).forEach(s=>{
      if(sel&&s.id===sel.id)return;
      out.push(s.ref+".response");
      if(s.type==="create-values"||s.type==="create-json")(s.props&&s.props.values||[]).forEach(v=>out.push(s.ref+"."+v.name));
      if(s.aggregation&&!s.aggregation.deleted)(s.aggregation.outputs||[]).forEach(o=>out.push((s.aggregation.ref||"Aggregate")+"."+o.name));
      if(s.body)walk(s.body);
    });
  }
  walk(ORCH.steps);
  return out;
}
function pickStrTok(key,val){sel.props[key]=sel.props[key]||[];sel.props[key].push({t:"string",v:val});afterPick(key);}
function pickRef(key,ref){sel.props[key]=sel.props[key]||[];sel.props[key].push({t:"ref",v:ref});afterPick(key);}
function afterPick(key){closePicker();(key==='path'?renderTok("tw_path",key,true):tfRefresh(key));save(true);}
function closePicker(){const p=document.getElementById("picker");if(p)p.remove();}
document.addEventListener("click",e=>{if(!e.target.closest(".picker")&&!e.target.closest(".addbtn")&&!e.target.closest(".insbtn"))closePicker();});

/* ---- actions: save / validate / build / deploy / run ---- */
function toast(m){const t=document.getElementById("toast");t.textContent=m;t.classList.add("show");setTimeout(()=>t.classList.remove("show"),1600);}
function save(silent){fetch("/orchestrate/api/save",{method:"POST",headers:{"Content-Type":"application/json"},
  body:JSON.stringify({appId:APPID,orch:ORCH})}).then(r=>r.json()).then(d=>{
    if(!silent){toast("Saved");}document.getElementById("saved").textContent="Saved to session less than a minute ago.";});}
function validate(){fetch("/orchestrate/api/validate",{method:"POST",headers:{"Content-Type":"application/json"},
  body:JSON.stringify({orch:ORCH})}).then(r=>r.json()).then(d=>{
    const b=document.getElementById("errBadge");
    if(d.ok){b.style.display="none";showConsole("build","<div class='ok'>&#10003; Validation passed.</div>");}
    else{b.style.display="flex";document.getElementById("errCount").textContent=d.issues.length;
      showConsole("build",d.issues.map(i=>"<div class='er'>&#9888; "+esc(i)+"</div>").join(""));}});}
function onDeploy(){
  const btn=document.getElementById("deployBtn");
  btn.textContent="Building";btn.classList.add("disabled");
  fetch("/orchestrate/api/build",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({appId:APPID,orch:ORCH})}).then(r=>r.json()).then(d=>{
      builtNo=d.buildNo;isBuilt=true;
      btn.innerHTML="Built &#10004;";btn.classList.remove("disabled");
      document.getElementById("saveAll").classList.add("disabled");
      document.getElementById("bn").textContent=d.buildNo;
      let h="";d.tenants.forEach(t=>{h+="<div class='trow'><b>"+t+"</b>"+
        "<button class='btn' onclick='doDeploy(\""+t+"\")'>Deploy</button></div>";});
      document.getElementById("tenantList").innerHTML=h;
      document.getElementById("deploy").classList.add("show");
    });
}
function doDeploy(tenant){
  const sysName=(prompt("Integration System Name (tenant task 'Create Integration System', template: Orchestrate Integration Template)","INT_"+ORCH.name)||"").trim();
  if(!sysName)return;
  fetch("/orchestrate/api/deploy",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify({appId:APPID,orch:ORCH,tenant:tenant,systemName:sysName})}).then(r=>r.json()).then(d=>{
      document.getElementById("deploy").classList.remove("show");
      toast("Deployed to "+tenant);
      showConsole("build","<div class='ok'>&#10003; Deployed '"+ORCH.name+"' to "+tenant+
        ".</div><div class='mut'>Tenant Integration System created: "+d.isId+
        "</div><div><a style='color:#7fb4ff' href='/orchestrate/tenant/integration-system/"+d.isId+
        "'>Open View Integration System &#8594;</a> &nbsp; then Actions &#8250; Integration &#8250; Launch/Schedule to produce RAAS DATA.</div>");
    });
}
function run(){fetch("/orchestrate/api/run",{method:"POST",headers:{"Content-Type":"application/json"},
  body:JSON.stringify({orch:ORCH})}).then(r=>r.json()).then(d=>{
    let h="<div><b>"+d.status.toUpperCase()+"</b> &middot; "+d.durationMs+" ms</div>";
    d.trace.forEach(e=>{h+="<div><span class='"+(e.status==='Error'?'er':'ok')+"'>["+e.status+"]</span> "+esc(e.step)+" <span class='mut'>"+esc(e.message)+"</span></div>";});
    d.outputFiles.forEach(f=>{h+="<div class='mut'>output file: "+esc(f.title)+" ("+f.type+")</div>";});
    showConsole("run",h);});}
function showConsole(kind,html){const c=document.getElementById("console");
  document.getElementById("consoleBody").innerHTML=(kind==='run'?"<div class='mut'>RUN LOGS</div>":"<div class='mut'>BUILD LOGS</div>")+html;
  c.classList.add("show");}
function toggleConsole(kind){if(kind==='run')run();else{const c=document.getElementById("console");c.classList.toggle("show");}}

/* ---- loop / log / aggregate property panels ---- */
function loopPropsHtml(){
  const p=sel.props;
  let h="<div class='fld'><label>Data Type <span class='req'>*</span></label>"+
    "<select onchange='setp(\"dataType\",this.value);save(true)'>"+
    opts(['AutoType Iterator','JSON Iterator','Text Iterator','Number Range'],p.dataType)+"</select></div>";
  h+="<div class='fld'><label>Data Set <span class='req'>*</span></label><div class='chiprow'>"+
     "<input class='chip cref' value='"+jv(p.dataSetRef)+"' placeholder='Step.field' oninput='setp(\"dataSetRef\",this.value)' onchange='save(true)'>"+
     "<span class='chip cfn'>iterator</span>"+
     "<input class='chip cjp' value='"+jv(p.dataSetPath)+"' placeholder='$.path[*]' oninput='setp(\"dataSetPath\",this.value)' onchange='save(true)'></div></div>";
  h+="<div class='fld'><label>Filter</label><div class='chiprow'>"+
     "<span class='chip citem'>Loop.item</span><span class='chip cfn'>booleanAtJsonPath</span>"+
     "<input class='chip cjp' value='"+jv(p.filterPath)+"' placeholder='$.active' oninput='setp(\"filterPath\",this.value)' onchange='save(true)'></div></div>";
  h+="<div class='fld'><label>Sort By</label>";
  (p.sortBy||[]).forEach((sb,i)=>{
    h+="<div class='chiprow'><span class='chip citem'>Loop.item</span><span class='chip cfn'>jsonDataAtJsonPath</span>"+
       "<input class='chip cjp' value='"+jv(sb.path)+"' placeholder='$.role' oninput='setSort("+i+",this.value)' onchange='save(true)'>"+
       "<span class='chip cfn'>toString</span>"+
       "<span class='sortdir' title='direction' onclick='toggleSort("+i+")'>"+(sb.dir==='desc'?'&#8595;':'&#8593;')+"</span>"+
       "<span class='chiprm' onclick='rmSort("+i+")'>&#10005;</span></div>";
  });
  h+="<a class='addlink' onclick='addSort()'>+ Add Sort By</a></div>";
  h+="<div class='fld'><label>Locale</label><div class='chiprow'>"+
     "<input class='chip cjp' style='min-width:140px' value='"+jv(p.locale)+"' placeholder='(optional)' oninput='setp(\"locale\",this.value)' onchange='save(true)'></div></div>";
  return h;
}
function setSort(i,v){sel.props.sortBy[i].path=v;}
function toggleSort(i){sel.props.sortBy[i].dir=(sel.props.sortBy[i].dir==='desc'?'asc':'desc');renderProps();save(true);}
function rmSort(i){sel.props.sortBy.splice(i,1);renderProps();save(true);}
function addSort(){sel.props.sortBy=sel.props.sortBy||[];sel.props.sortBy.push({path:"",dir:"asc"});renderProps();save(true);}

function logPropsHtml(){
  const p=sel.props;
  let h="<div class='fld'><label>Message <span class='req'>*</span></label><div class='chiprow'>"+
    "<input class='chip' style='flex:1' value='"+jv(p.message!=null?p.message:'')+"' placeholder='USER ADDED - ORCHESTRATION HIT' oninput='setp(\"message\",this.value)' onchange='save(true)'></div></div>";
  h+="<div class='fld'><label>Or reference a step value</label><div class='chiprow'>"+
    "<input class='chip cref' value='"+jv(p.messageRef||'')+"' placeholder='Loop.item' oninput='setp(\"messageRef\",this.value)' onchange='save(true)'>"+
    "<span class='chip cfn'>toString</span></div></div>";
  h+="<div class='fld'><label>Condition <span class='req'>*</span></label><div class='chiprow'>"+
    "<input class='chip cbool' value='"+jv(p.condition||'true')+"' oninput='setp(\"condition\",this.value)' onchange='save(true)'></div></div>";
  return h;
}

/* =====================================================================
   Aggregate full-screen editor -- replicates the Orchestrate deep-dive:
   outputs table, Custom strategy (Output Type / Initial Value / Next
   Value), JSON Fragment, Condition gate, pill picker with
   "Add string" / "Add number", expression (text) mode, and the
   "Add Conditions for Value" modal.
   ===================================================================== */
const AGG_FNS=["asJSON","addStringValue","addNumberValue","addBooleanValue","concat","format","toString","upper","lower","random","randomUUID"];
const AGG_OPS=["Is equal to","Is not equal to","Contains","Does not contain","Starts with","Greater than","Less than","Is empty","Is not empty"];
let aggSel=0, CW=null, condTarget=null, pickPath=null, PICKQ="", PICKALL=false;

function normOut(o){
  o.strategy=o.strategy||"JSON";o.outputType=o.outputType||"JSON";
  ["initialValue","nextValue","jsonFragment","condition"].forEach(k=>{if(!Array.isArray(o[k]))o[k]=[];});
  return o;
}
function cssId(p){return p.replace(/\./g,"_");}
function getExpr(path){
  const p=path.split(".");
  if(p[0]==="o")return sel.aggregation.outputs[+p[1]][p[2]];
  if(p[0]==="cw"){if(p[1]==="rows")return CW.rows[+p[2]][p[3]];return CW[p[1]];}
  return [];
}
function rerenderCtx(path){if(path.indexOf("cw")===0)renderCondModal();else renderAggPanel();}

function renderAggProps(){document.getElementById("props").classList.remove("show");renderAggPanel();}
function closeAggPanel(){document.getElementById("aggPanel").classList.remove("show");deselect();}

function aggRefs(){
  const refs=["Loop.item","Loop.itemNumber","Loop.index"];
  const aref=(sel&&sel.aggregation&&sel.aggregation.ref)||"Aggregate";
  ((sel&&sel.aggregation&&sel.aggregation.outputs)||[]).forEach(o=>refs.push(aref+"."+o.name));
  (ORCH.steps||[]).forEach(s=>{
    if(s.ref&&!["loop","batch-loop","join-loop"].includes(s.type)){refs.push(s.ref+".message");refs.push(s.ref+".response");}
  });
  refs.push("Input");
  return refs;
}
function refLike(q){
  /* A typed dotted path under a known data root becomes a reference pill,
     e.g. Loop.item.Emergency_Contact_Phone or SendWorkdayRaaSRequest.response.Report_Entry */
  if(!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z0-9_]+)+$/.test(q))return false;
  const root=q.split(".")[0];
  const aref=(sel&&sel.aggregation&&sel.aggregation.ref)||"Aggregate";
  const roots=["Loop","Input",aref].concat((ORCH.steps||[]).map(s=>s.ref).filter(Boolean));
  return roots.indexOf(root)>=0;
}

/* ---- pill rendering ---- */
function pillHTML(tok,path,idx){
  const rm="<span class='prm' onclick='event.stopPropagation();rmPill(\""+path+"\","+idx+")'>&#10005;</span>";
  if(tok.t==="json")return "<span class='pl pjson'>{&nbsp;}"+rm+"</span>";
  if(tok.t==="fn")return "<span class='pl pfn'>"+esc(tok.v)+rm+"</span>";
  if(tok.t==="ref"){
    const aref=(sel&&sel.aggregation&&sel.aggregation.ref)||"Aggregate";
    const cls=(tok.v||"").indexOf(aref+".")===0?"pagg":"pref";
    return "<span class='pl "+cls+"'>"+esc(tok.v)+rm+"</span>";
  }
  if(tok.t==="str"||tok.t==="num")return "<span class='pl plit'>"+esc(tok.v)+rm+"</span>";
  if(tok.t==="open")return "<span class='pl ppar'>("+rm+"</span>";
  if(tok.t==="close")return "<span class='pl ppar'>)"+rm+"</span>";
  if(tok.t==="cond")return "<span class='pl pcond' title='Conditional value (click to edit)' "+
    "onclick='openCondModal(\""+path+"\","+idx+")'>&#8916; if&#8230;else"+rm+"</span>";
  return "";
}
function pillbox(path,o){
  o=o||{};const arr=getExpr(path);
  const raw=arr.length===1&&arr[0].t==="raw";
  let inner="";
  if(raw){
    inner="<input class='rawexp' value='"+jv(arr[0].v)+"' "+
      "oninput='getExpr(\""+path+"\")[0].v=this.value;aggFieldErr(\""+path+"\")' onchange='save(true)'>";
  }else{
    arr.forEach((t,i)=>inner+=pillHTML(t,path,i));
    inner+="<button class='addbtn' title='Add new expression' onclick='openPick(event,\""+path+"\")'>+</button>";
    if(o.condBtn)inner+="<button class='insbtn' title='Add Conditions for Value' onclick='openCondModal(\""+path+"\",-1)'>&#8916;</button>";
  }
  const msg=exprErrText(arr);
  return "<div class='pbox"+(msg?" errb":"")+"' id='pb_"+cssId(path)+"'>"+inner+
    "<span class='dots aggmenu' onclick='exprMenu(event,\""+path+"\")'>&#8942;</span></div>"+
    "<div class='experr' id='er_"+cssId(path)+"'>"+esc(msg)+"</div>";
}
function rmPill(path,i){getExpr(path).splice(i,1);rerenderCtx(path);save(true);}

/* ---- validation (mirrors builder error strings) ---- */
function exprErrText(arr){
  if(arr.length===1&&arr[0].t==="raw"){
    const t=arr[0].v||"";let d=0;
    for(const c of t){if(c==="(")d++;if(c===")"){d--;if(d<0)return "Error in expression: Native parser error: ) expected but string constant found";}}
    if(d>0)return "Error in expression: Native parser error: ( expected but end of expression found";
    return "";
  }
  for(const t of arr){
    if(t.t==="cond"){
      if(!(t.ifTrue||[]).length||!(t.ifFalse||[]).length)return "Error in expression: Expression contains a placeholder";
      for(const r of (t.rows||[])){
        const needsRight=!(r.op==="Is empty"||r.op==="Is not empty");
        if(!(r.left||[]).length||(needsRight&&!(r.right||[]).length))return "Error in expression: Expression contains a placeholder";
      }
    }
  }
  return "";
}
function aggFieldErr(path){
  const el=document.getElementById("er_"+cssId(path));if(!el)return;
  const msg=exprErrText(getExpr(path));
  el.textContent=msg;
  const box=document.getElementById("pb_"+cssId(path));if(box)box.classList.toggle("errb",!!msg);
}
function outErrCount(o){
  const flds=o.strategy==="Custom"?["initialValue","nextValue","condition"]:
             o.strategy==="JSON"?["jsonFragment","condition"]:[];
  let n=0;flds.forEach(f=>{if(exprErrText(o[f]||[]))n++;});return n;
}

/* ---- main panel ---- */
function renderAggPanel(){
  const agg=sel.aggregation;(agg.outputs||[]).forEach(normOut);
  if(aggSel>=agg.outputs.length)aggSel=Math.max(0,agg.outputs.length-1);
  let h="<div class='aph'><span class='apic'>"+SVG_BRANCH+"</span><b style='font-size:17px'>Aggregate</b>"+
    "<span class='ed' style='margin-left:auto'>Editing</span><span style='color:#9aa3b0'>&#128221;</span>"+
    "<span style='color:#9aa3b0'>&#8689;</span><span class='x' style='cursor:pointer' onclick='closeAggPanel()'>&#10005;</span></div>";
  h+="<div class='aprow'><label>Reference Name <span class='req'>*</span></label>"+
     "<div class='apref'><input value='"+jv(agg.ref)+"' oninput='sel.aggregation.ref=this.value' onchange='save(true)'>"+
     "<span class='inf'>&#9432;</span></div></div>";
  h+="<div class='apcols'><div class='apleft'>";
  h+="<div class='apadd'>Add outputs from the data to aggregate. <span class='inf'>&#9432;</span></div>";
  h+="<table class='aggtbl big'><tr><th>Name</th><th>Strategy</th><th>Delete</th><th>Errors</th></tr>";
  agg.outputs.forEach((o,i)=>{
    const errn=outErrCount(o);
    h+="<tr class='"+(i===aggSel?"selrow":"")+"' onclick='aggSel="+i+";renderAggPanel()'>"+
      "<td><input value='"+jv(o.name)+"' onclick='event.stopPropagation()' oninput='setAgg("+i+",\"name\",this.value)' onchange='save(true);renderAggPanel()'></td>"+
      "<td><select onclick='event.stopPropagation()' onchange='setAgg("+i+",\"strategy\",this.value);aggSel="+i+";save(true);renderAggPanel()'>"+opts(["Custom","JSON","CSV","Text","Count"],o.strategy)+"</select></td>"+
      "<td style='text-align:center'><span class='trash' onclick='event.stopPropagation();rmAgg("+i+")'>&#128465;</span></td>"+
      "<td style='text-align:center'>"+(errn?"<span class='errn'>&#9888; "+errn+"</span>":"")+"</td></tr>";
  });
  h+="</table><a class='addlink' onclick='addAgg()'>+ Add Output</a>";
  h+="<div class='aggopt'><label>Fail When No Inputs</label>"+
     "<input type='checkbox' "+(agg.failWhenNoInputs?"checked":"")+" onchange='sel.aggregation.failWhenNoInputs=this.checked;save(true)'></div>";
  if(agg.earlyStop!=null){
    h+="<div class='fld' style='margin-top:14px'><label>Early Stop Condition (booleanAtJsonPath on item)</label>"+
       "<input style='width:100%;border:1px solid #c4ccd6;border-radius:8px;padding:8px' value='"+jv(agg.earlyStop)+"' placeholder='$.stop' oninput='sel.aggregation.earlyStop=this.value' onchange='save(true)'></div>";
  }
  if(agg.errorHandler)h+="<p class='hint'>Error Handler attached to this aggregation.</p>";
  h+="</div><div class='apright'>";
  if(!agg.outputs.length){
    h+="<div class='aggempty'><b>No Output Strategy Selected</b><br>Configuration options will appear once an output with a strategy is added.</div>";
  }else{
    const o=agg.outputs[aggSel],b="o."+aggSel+".";
    h+="<div class='apcfg'>Configure selected output: <b>"+esc(o.name)+"</b></div>";
    if(o.strategy==="Custom"){
      h+="<div class='fld'><label>Output Type <span class='req'>*</span></label>"+
         "<select style='max-width:300px;border:1px solid #c4ccd6;border-radius:8px;padding:9px;font-family:inherit' "+
         "onchange='setAgg("+aggSel+",\"outputType\",this.value);save(true)'>"+opts(["JSON","Text","Number"],o.outputType)+"</select></div>";
      h+="<div class='fld'><label>Initial Value <span class='req'>*</span></label>"+pillbox(b+"initialValue",{})+"</div>";
      h+="<div class='fld'><label>Next Value <span class='req'>*</span></label>"+pillbox(b+"nextValue",{})+"</div>";
    }else if(o.strategy==="JSON"){
      h+="<div class='fld'><label>JSON Fragment <span class='req'>*</span></label>"+pillbox(b+"jsonFragment",{})+"</div>";
    }else if(o.strategy==="CSV"){
      h+="<div class='fld'><label>CSV Columns <span class='req'>*</span></label>"+
         "<textarea class='code' spellcheck='false' style='min-height:90px' placeholder='employeeId, prefFirstName, legalFirstName' "+
         "oninput='setAgg("+aggSel+",\"csvColumns\",this.value)' onchange='save(true)'>"+esc(o.csvColumns||'')+"</textarea>"+
         "<p class='hint'>Comma-separated. A bare name reads <code>Loop.item.&lt;name&gt;</code> (must match the report column label exactly). A dotted path reads the full context.</p></div>";
      h+="<div class='fld'><label>Delimiter</label><input style='width:80px' value='"+jv(o.csvDelimiter||',')+"' oninput='setAgg("+aggSel+",\"csvDelimiter\",this.value)' onchange='save(true)'></div>";
      h+="<div class='fld'><label style='display:inline-flex;align-items:center;gap:8px'><input type='checkbox' "+(o.csvHeader?'checked':'')+
         " onchange='setAgg("+aggSel+",\"csvHeader\",this.checked);save(true)'> Include header row</label></div>";
    }else{
      h+="<p class='hint'>"+(o.strategy==="Text"?"Each aggregated input is appended as a line of text.":"Counts the number of aggregated inputs.")+"</p>";
    }
    if(o.strategy==="Custom"||o.strategy==="JSON"||o.strategy==="CSV"){
      h+="<div class='fld'><label>Condition</label>"+pillbox(b+"condition",{condBtn:1})+"</div>";
      h+="<div class='apinfo'><span style='color:#0875e1'>&#9432;</span>&nbsp; Input will only aggregate if expression entered in <b>Condition</b> returns as true. Entering a <b>Condition</b> is optional.</div>";
    }
  }
  h+="</div></div>";
  h+="<div class='apfoot'><button class='btn' onclick='closeAggPanel()'>Close</button></div>";
  const el=document.getElementById("aggPanel");
  el.innerHTML=h;el.classList.add("show");
}
function setAgg(i,k,v){sel.aggregation.outputs[i][k]=v;}
function rmAgg(i){sel.aggregation.outputs.splice(i,1);renderAggPanel();save(true);}
function addAgg(){
  sel.aggregation.outputs.push(normOut({name:"Output"+(sel.aggregation.outputs.length+1),strategy:"Custom"}));
  aggSel=sel.aggregation.outputs.length-1;renderAggPanel();save(true);
}

/* ---- picker: Add string / Add number / functions / data refs ---- */
function openPick(ev,path){
  closePick();pickPath=path;PICKQ="";PICKALL=false;
  const pop=document.createElement("div");pop.id="pickpop";pop.className="picker pickpop";
  pop.style.position="fixed";
  pop.innerHTML="<div class='typ'><input id='pickIn' placeholder='Type to add a value or search'></div><div id='pickList'></div>";
  document.body.appendChild(pop);
  const r=ev.target.getBoundingClientRect();
  pop.style.left=Math.min(r.left,window.innerWidth-460)+"px";
  pop.style.top=Math.min(r.bottom+4,window.innerHeight-340)+"px";
  const inp=document.getElementById("pickIn");
  inp.oninput=function(){PICKQ=inp.value;pickList();};
  setTimeout(()=>inp.focus(),0);
  pickList();
}
function pickList(){
  const q=PICKQ.trim(),ql=q.toLowerCase();let h="";
  if(q){
    if(refLike(q))
      h+="<div class='sub' onclick='pickTok({t:\"ref\",v:\""+jv(q)+"\"})'>+ <b>Add reference</b> <span class='stepname'>"+esc(q)+"</span></div>";
    h+="<div class='sub' onclick='pickStr()'>+ <b>Add string</b> &quot;"+esc(q)+"&quot;</div>";
    if(!isNaN(q))h+="<div class='sub' onclick='pickNum()'>+ <b>Add number</b> "+esc(q)+"</div>";
  }
  if(!ql||"{} json".indexOf(ql)>=0)
    h+="<div class='sub' onclick='pickTok({t:\"json\"})'><b>{ }</b><i>Empty JSON</i></div>";
  aggRefs().filter(f=>!ql||f.toLowerCase().indexOf(ql)>=0)
    .forEach(rf=>h+="<div class='sub' onclick='pickTok({t:\"ref\",v:\""+jv(rf)+"\"})'><span class='stepname'>"+esc(rf)+"</span><i>data reference</i></div>");
  AGG_FNS.filter(f=>PICKALL||!ql||f.toLowerCase().indexOf(ql)>=0)
    .forEach(fn=>h+="<div class='sub' onclick='pickTok({t:\"fn\",v:\""+jv(fn)+"\"})'><b>"+esc(fn)+"</b><i>function</i></div>");
  if(!h)h="<div class='noRes'>No Results Found.</div>";
  h+="<div class='add' onclick='PICKALL=true;PICKQ=\"\";document.getElementById(\"pickIn\").value=\"\";pickList()'>&#128269;&nbsp;Explore All Functions</div>";
  document.getElementById("pickList").innerHTML=h;
}
function pickStr(){pickTok({t:"str",v:PICKQ.trim()});}
function pickNum(){pickTok({t:"num",v:PICKQ.trim()});}
function pickTok(tok){
  const p=pickPath;const arr=getExpr(p);arr.push(tok);
  closePick();rerenderCtx(p);save(true);
}
function closePick(){const m=document.getElementById("pickpop");if(m)m.remove();}
document.addEventListener("click",function(e){
  if(!e.target.closest(".pickpop")&&!e.target.closest(".addbtn"))closePick();
});

/* ---- expression (text) mode <-> pill mode ---- */
function exprMenu(ev,path){
  closePick();closeAggMenu();
  const arr=getExpr(path);const raw=arr.length===1&&arr[0].t==="raw";
  const m=document.createElement("div");m.className="aggmenupop";m.id="aggmenupop";
  m.innerHTML="<div class='ami' onclick=\"closeAggMenu();switchMode('"+path+"')\">"+(raw?"Switch to Pill Mode":"Switch to Expression Mode")+"</div>"+
    "<div class='ami' onclick=\"closeAggMenu();clearExpr('"+path+"')\">Clear Expression</div>";
  document.body.appendChild(m);
  const r=ev.target.getBoundingClientRect();
  m.style.left=Math.min(r.left,window.innerWidth-240)+"px";m.style.top=(r.bottom+4)+"px";
}
function clearExpr(path){const a=getExpr(path);a.length=0;rerenderCtx(path);save(true);}
function switchMode(path){
  const arr=getExpr(path);
  if(arr.length===1&&arr[0].t==="raw"){
    const p=toPills(arr[0].v);
    if(p===null){toast("The current expression is not supported in pill mode");return;}
    arr.length=0;p.forEach(t=>arr.push(t));
  }else{
    const txt=toText(arr);
    arr.length=0;arr.push({t:"raw",v:txt});
  }
  rerenderCtx(path);save(true);
}
function argText(t){
  if(t.t==="ref")return "data."+t.v;
  if(t.t==="str")return '"'+t.v+'"';
  if(t.t==="num")return String(t.v);
  if(t.t==="json")return "{}";
  if(t.t==="fn")return t.v+"()";
  if(t.t==="cond"){
    const r=(t.rows&&t.rows[0])||{left:[],op:"Is equal to",right:[]};
    const OP={"Is equal to":"==","Is not equal to":"!="}[r.op]||"==";
    const seg=a=>(a||[]).map(argText).join(" + ")||'""';
    return "(("+seg(r.left)+" "+OP+" "+seg(r.right)+") "+seg(t.ifTrue)+" else "+seg(t.ifFalse)+")";
  }
  return "";
}
function toText(arr){
  let out="",i=0;
  while(i<arr.length){
    const t=arr[i];
    if(t.t==="fn"&&(t.v==="addStringValue"||t.v==="addNumberValue")){
      out+="."+t.v+"("+arr.slice(i+1).filter(x=>x.t!=="open"&&x.t!=="close").map(argText).join(", ")+")";
      break;
    }else if(t.t==="fn"&&t.v==="asJSON"){out+=".asJSON()";i++;}
    else if(t.t==="json"){out+="{}";i++;}
    else if(t.t==="ref"){out+=(out&&!out.endsWith("()")&&out!=="{}"?" + ":"")+"data."+t.v;i++;}
    else if(t.t==="str"){out+=(out?" + ":"")+'"'+t.v+'"';i++;}
    else if(t.t==="num"){out+=(out?" + ":"")+t.v;i++;}
    else if(t.t==="cond"){out+=(out?" + ":"")+argText(t);i++;}
    else i++;
  }
  return out;
}
function splitArgsJS(s){
  const out=[];let d=0,q=null,cur="";
  for(const c of s){
    if(q){cur+=c;if(c===q)q=null;continue;}
    if(c==='"'||c==="'"){q=c;cur+=c;continue;}
    if(c==="(")d++;if(c===")")d--;
    if(c===","&&d===0){out.push(cur);cur="";}else cur+=c;
  }
  if(cur.trim())out.push(cur);
  return out;
}
function refTok(s){
  if(/^data\.[A-Za-z0-9_.]+$/.test(s))return {t:"ref",v:s.slice(5)};
  if(/^[A-Za-z0-9_.]+$/.test(s))return {t:"ref",v:s};
  return null;
}
function scalarTok(s){
  s=s.trim();
  if(/^".*"$/s.test(s))return {t:"str",v:s.slice(1,-1)};
  if(/^'.*'$/s.test(s))return {t:"str",v:s.slice(1,-1)};
  if(/^-?[0-9.]+$/.test(s))return {t:"num",v:s};
  if(s==="{}")return {t:"json"};
  if(/^(randomUUID|random)\(\)$/.test(s))return {t:"fn",v:s.replace("()","")};
  return refTok(s);   // ternaries and anything else are unsupported in pill mode
}
function toPills(text){
  let s=(text||"").trim();if(!s)return [];
  const out=[];
  if(s.indexOf("{}")===0){out.push({t:"json"});s=s.slice(2);}
  if(s.indexOf(".asJSON()")===0){out.push({t:"fn",v:"asJSON"});s=s.slice(9);}
  const m=s.match(/^(.*?)\.(addStringValue|addNumberValue)\((.*)\)\s*$/s);
  if(m){
    if(m[1].trim()){const t=refTok(m[1].trim());if(!t)return null;out.push(t);}
    out.push({t:"fn",v:m[2]});
    for(const a of splitArgsJS(m[3])){const t=scalarTok(a);if(!t)return null;out.push(t);}
    return out;
  }
  if(s.trim()){const t=scalarTok(s);if(!t)return null;out.push(t);}
  return out;
}

/* ---- "Add Conditions for Value" modal ---- */
function openCondModal(path,idx){
  closePick();
  condTarget={path:path,idx:idx};
  const arr=getExpr(path);
  const base=(idx>=0&&arr[idx]&&arr[idx].t==="cond")?arr[idx]:null;
  CW=base?JSON.parse(JSON.stringify(base))
         :{t:"cond",mode:"all",rows:[{left:[],op:"Is equal to",right:[]}],ifTrue:[],ifFalse:[]};
  renderCondModal();
}
function renderCondModal(){
  const el=document.getElementById("condModal");
  let h="<div class='conddlg'><span class='cx' onclick='cancelCond()'>&#10005;</span>";
  h+="<h2>Add Conditions for Value</h2>";
  h+="<div class='condsec'>Select one</div>";
  h+="<div class='radio "+(CW.mode==="all"?"on":"")+"' onclick='CW.mode=\"all\";renderCondModal()'><span class='rd'></span>All of these conditions must be met</div>";
  h+="<div class='radio "+(CW.mode==="any"?"on":"")+"' onclick='CW.mode=\"any\";renderCondModal()'><span class='rd'></span>One of these conditions must be met</div>";
  h+="<div class='condsec'>Conditions</div>";
  h+="<table class='condtbl'><tr><th style='width:33%'>Value</th><th style='width:20%'>Condition</th><th style='width:33%'>Value</th><th>Delete</th></tr>";
  CW.rows.forEach((r,i)=>{
    const noRight=(r.op==="Is empty"||r.op==="Is not empty");
    h+="<tr><td>"+pillbox("cw.rows."+i+".left",{})+"</td>";
    h+="<td><select onchange='CW.rows["+i+"].op=this.value;renderCondModal()'>"+opts(AGG_OPS,r.op)+"</select></td>";
    h+="<td>"+(noRight?"<span class='hint'>(no value needed)</span>":pillbox("cw.rows."+i+".right",{}))+"</td>";
    h+="<td style='text-align:center'><span class='trash' onclick='CW.rows.splice("+i+",1);renderCondModal()'>&#128465;</span></td></tr>";
  });
  h+="</table><a class='addlink' onclick='CW.rows.push({left:[],op:\"Is equal to\",right:[]});renderCondModal()'>+ Add Value</a>";
  h+="<div class='condsec'>If conditions are true</div>"+pillbox("cw.ifTrue",{});
  h+="<div class='condsec'>If conditions are false</div>"+pillbox("cw.ifFalse",{});
  h+="<button class='savebtn' onclick='saveCond()'>Save</button>";
  h+="</div>";
  el.innerHTML=h;el.classList.add("show");
}
function cancelCond(){CW=null;document.getElementById("condModal").classList.remove("show");}
function saveCond(){
  const arr=getExpr(condTarget.path);
  const tok=JSON.parse(JSON.stringify(CW));
  if(condTarget.idx>=0&&arr[condTarget.idx]&&arr[condTarget.idx].t==="cond")arr[condTarget.idx]=tok;
  else arr.push(tok);
  CW=null;document.getElementById("condModal").classList.remove("show");
  renderAggPanel();save(true);
}

/* ---- create values (JSON path extraction) ---- */
function setCV(i,k,v){sel.props.values[i][k]=v;}
function rmCV(i){sel.props.values.splice(i,1);renderProps();save(true);}
function addCV(){sel.props.values=sel.props.values||[];sel.props.values.push({name:"Value"+(sel.props.values.length+1),sourceRef:"SendHTTPRequest.response",jsonPath:"$."});renderProps();save(true);}

/* ---- assertions (Continue on Conditions) ---- */
function setAS(i,k,v){sel.props.assertions[i][k]=v;}
function rmAS(i){sel.props.assertions.splice(i,1);renderProps();save(true);}
function addAS(){sel.props.assertions=sel.props.assertions||[];sel.props.assertions.push({expr:"",message:""});renderProps();save(true);}

/* ---- query parameters (Send HTTP Request) ---- */
function setQP(i,k,v){sel.props.queryParams[i][k]=v;}
function rmQP(i){sel.props.queryParams.splice(i,1);renderProps();save(true);}
function addQP(){sel.props.queryParams=sel.props.queryParams||[];sel.props.queryParams.push({key:"",value:""});renderProps();save(true);}

/* ---- aggregation context menu ---- */
function toggleAggMenu(ev,loopId){
  if(document.getElementById("aggmenupop")){closeAggMenu();return;}
  const items=[["Delete","removeStep('"+loopId+"')"],["Rename","selectAgg('"+loopId+"')"],
    ["Duplicate",""],["Open Node","selectAgg('"+loopId+"')"],["Delete Aggregation","delAgg('"+loopId+"')"],
    ["Add Early Stop Condition","addEarlyStop('"+loopId+"')"],["Add Error Handler","addErrHandler('"+loopId+"')"],["Disable",""]];
  const m=document.createElement("div");m.className="aggmenupop";m.id="aggmenupop";
  m.innerHTML=items.map(it=>"<div class='ami' onclick=\"closeAggMenu();"+it[1]+"\">"+it[0]+"</div>").join("");
  document.body.appendChild(m);
  const r=ev.target.getBoundingClientRect();
  m.style.left=Math.min(r.left,window.innerWidth-220)+"px";m.style.top=(r.bottom+4)+"px";
}
function closeAggMenu(){const m=document.getElementById("aggmenupop");if(m)m.remove();}
function delAgg(id){const l=findStep(ORCH.steps,id);if(l&&l.aggregation)l.aggregation.deleted=true;if(sel&&sel.id===id&&selKind==="aggregate")deselect();render();save(true);}
function addEarlyStop(id){const l=findStep(ORCH.steps,id);if(l&&l.aggregation){l.aggregation.deleted=false;if(l.aggregation.earlyStop==null)l.aggregation.earlyStop="$.stop";}selectAgg(id);save(true);}
function addErrHandler(id){const l=findStep(ORCH.steps,id);if(l&&l.aggregation){l.aggregation.deleted=false;l.aggregation.errorHandler=true;}selectAgg(id);save(true);}
document.addEventListener("click",function(e){if(!e.target.closest(".aggmenupop")&&!e.target.closest(".aggmenu"))closeAggMenu();});

/* ---- drop steps into a loop ---- */
function attachLoopDnD(){
  document.querySelectorAll(".loopwrap").forEach(w=>{
    w.ondragover=function(e){e.preventDefault();e.stopPropagation();w.classList.add("over");};
    w.ondragleave=function(){w.classList.remove("over");};
    w.ondrop=function(e){e.preventDefault();e.stopPropagation();w.classList.remove("over");
      if(dragType){addToLoop(w.dataset.loop,dragType);dragType=null;}};
  });
}

buildPalette();render();
</script>
</body></html>
"""
