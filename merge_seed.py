"""Adds the Employee Demographic Outbound app to orchestrate_store.json WITHOUT touching existing apps.
Usage (from the mock-workday folder):  python merge_seed.py
"""
import json, shutil, sys, os
STORE = "orchestrate_store.json"
SEED = "employee_demographic_seed.json"
if not os.path.exists(STORE):
    sys.exit("orchestrate_store.json not found in this folder")
shutil.copy(STORE, STORE + ".bak")
d = json.load(open(STORE, encoding="utf-8"))
app = json.load(open(SEED, encoding="utf-8"))
d.setdefault("apps", {})
if app["id"] in d["apps"]:
    print("App already present, replacing only its orchestration EmployeeDemographicFull")
    d["apps"][app["id"]]["orchestrations"]["EmployeeDemographicFull"] = app["orchestrations"]["EmployeeDemographicFull"]
else:
    d["apps"][app["id"]] = app
    print("App added:", app["name"])
json.dump(d, open(STORE, "w", encoding="utf-8"), indent=2)
print("Backup written to", STORE + ".bak")
