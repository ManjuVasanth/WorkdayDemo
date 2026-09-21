"""Seed the INT_Gympass_OTP Studio project (PCAOB Gympass one-time payment).

MERGES into studio_projects.json - your existing saved projects are kept.

Run from the mock-workday folder:
    python seed_gympass.py
Then restart the server (python workday_ui.py), open Workday Studio,
and open project INT_Gympass_OTP.

Flow (mirrors the real integration, SFTP/PGP stubbed by local-in):
  workday-in (launch params: pay_period, source)
  local-in gympass_payments.csv          <- stub: sftp-get + PGP decrypt
  [mediation ValidateAndParse: store, csv-to-xml]
  splitter (Row)
     route   - validation router: filters unknown worker 88888 (invalid path)
     xslt    - Row -> Request_One_Time_Payment SOAP
     workday-out-soap Compensation/Request_One_Time_Payment (send_message)
              - row 99999 faults here = record-level technical error
  aggregator (Responses)
  [mediation FinalizeRun: put-integration-message]
  write gympass_responses.xml            <- WS responses archive\n  write gympass_errors.csv (content=errors) <- error file back to vendor
  global-error-handler -> log-error -> send-error   (SendError lane)
"""
import json
import os

PROJECT = "INT_Gympass_OTP"

GYMPASS_XSL = """<?xml version="1.0"?>
<xsl:stylesheet version="1.0"
    xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:output method="xml"/>
<xsl:template match="/Row">
<env:Envelope xmlns:env="http://schemas.xmlsoap.org/soap/envelope/"
              xmlns:wd="urn:com.workday/bsvc"><env:Body>
<wd:Request_One_Time_Payment_Request wd:version="v42.0">
  <wd:One_Time_Payment_Data>
    <wd:Employee_Reference>
      <wd:ID wd:type="Employee_ID"><xsl:value-of select="Employee_ID"/></wd:ID>
    </wd:Employee_Reference>
    <wd:One_Time_Payment_Plan>
      <xsl:value-of select="One_Time_Payment_Plan"/>
    </wd:One_Time_Payment_Plan>
    <wd:Reason_Code><xsl:value-of select="Reason_Code"/></wd:Reason_Code>
    <wd:Amount><xsl:value-of select="Amount"/></wd:Amount>
    <wd:Currency><xsl:value-of select="Currency"/></wd:Currency>
    <wd:Effective_Date><xsl:value-of select="Effective_Date"/></wd:Effective_Date>
  </wd:One_Time_Payment_Data>
</wd:Request_One_Time_Payment_Request>
</env:Body></env:Envelope>
</xsl:template></xsl:stylesheet>"""

ASSEMBLY = [
    {"type": "workday_in", "props": {
        "service": "INT_Gympass_OTP",
        "launch_params": "pay_period:date=2026-06-30\n"
                         "source:text=Gympass SFTP (PGP stub)"}},
    {"type": "wd_out_soap", "props": {
        "service": "Human_Resources", "operation": "Get_Workers",
        "count": "30"}},
    {"type": "eval", "props": {"script":
        "props['hashMap'] = vars.workerIds; "
        "vars.eligibleCount = size(props['hashMap'])"}},
    {"type": "local_in", "props": {"filename": "gympass_payments.csv"}},
    {"type": "mediation", "props": {"name": "ValidateAndParse",
                                    "on_error": "stop"}},
    {"type": "store", "props": {"label": "gympass-input"}},
    {"type": "csv_to_xml", "props": {"delimiter": ","}},
    {"type": "splitter", "props": {"element": "Row"}},
    {"type": "route", "props": {
        "condition":
            "props['hashMap'] contains tagvalue('Employee_ID')"}},
    {"type": "xslt", "props": {"stylesheet": GYMPASS_XSL}},
    {"type": "wd_out_soap", "props": {
        "service": "Compensation",
        "operation": "Request_One_Time_Payment",
        "send_message": "true"}},
    {"type": "aggregator", "props": {"wrapper": "Responses"}},
    {"type": "mediation", "props": {"name": "FinalizeRun",
                                    "on_error": "stop"}},
    {"type": "pim", "props": {
        "severity": "Info",
        "message": "Gympass OTP load complete for @{props.pay_period} "
                   "(source: @{props.source})"}},
    {"type": "write_file", "props": {"filename": "gympass_responses.xml"}},
    {"type": "write_file", "props": {"filename": "gympass_errors.csv",
                                     "content": "errors"}},
    {"type": "global_error_handler", "props": {"name": "glob_err"}},
    {"type": "log_error", "props": {
        "message": "Caught: @{vars._lasterror}"}},
    {"type": "send_error", "props": {
        "message": "Gympass integration failed: @{vars._lasterror}",
        "mark_failed": "true"}},
]


def main():
    path = "studio_projects.json"
    projects = {}
    if os.path.exists(path):
        with open(path) as f:
            projects = json.load(f)
        print(f"Existing projects kept: {list(projects)}")
    projects[PROJECT] = ASSEMBLY          # merge, don't overwrite
    with open(path, "w") as f:
        json.dump(projects, f, indent=1)
    print(f"Seeded {PROJECT} ({len(ASSEMBLY)} steps) into {path}")
    if not os.path.exists("gympass_payments.csv"):
        print("WARNING: gympass_payments.csv not found next to this script - "
              "download it too.")


if __name__ == "__main__":
    main()
