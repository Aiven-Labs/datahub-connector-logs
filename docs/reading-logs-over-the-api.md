# **Reading ingestion logs over the API**

How to fetch ingestion run history and the logs of failed runs programmatically, without using the UI. Everything here is stock DataHub — no extra integration, no configuration to enable.

Every command below was verified against DataHub 1.7.0.1.

## **What is actually stored**

Each ingestion run is a dataHubExecutionRequest entity. Its result holds:

| Field | What it is |
| :---- | :---- |
| status | SUCCESS, FAILURE, RUNNING, TIMEOUT, CANCELLED, ABORTED |
| startTimeMs, durationMs | When it ran and for how long |
| report | The log text — the same content the UI's \*download logs\* button gives you |
| structuredReport | A JSON rendering of the same run |

report is the field to build on. It contains the datahub ingest output including Python stacktraces, which is what you need to tell \*why\* a run failed.

## **1\. Get an access token**

In the UI, go to **Settings → Access Tokens → Generate new token**. Copy it immediately; it is shown only once.

The token inherits its owner's privileges, and the user needs the **Manage Ingestion** platform privilege to see ingestion sources and their runs. Without it, the queries below return null rather than an error, which is easy to mistake for "no data".

## **2\. The endpoint**

POST https\://\<your-datahub-url\>/api/graphql  
Authorization: Bearer \<token\>

Content-Type: application/json

Use the same host you log into. Check the token works before going further:

export DATAHUB\_URL="https\://\<your-datahub-url\>"  
export DATAHUB\_TOKEN="\<your token\>"

curl \-sS \-X POST "\$DATAHUB\_URL/api/graphql" \\  
  \-H "Authorization: Bearer \$DATAHUB\_TOKEN" \\  
  \-H "Content-Type: application/json" \\

  \-d '{"query":"{ me { corpUser { urn } platformPrivileges { manageIngestion } } }"}'

Expect manageIngestion to be true:

{"data":{"me":{"corpUser":{"urn":"urn:li:corpuser:you"},"platformPrivileges":{"manageIngestion":true}}}}

## **3\. List your ingestion sources**

You need a source's urn to ask about its runs.

curl \-sS \-X POST "\$DATAHUB\_URL/api/graphql" \\  
  \-H "Authorization: Bearer \$DATAHUB\_TOKEN" \\  
  \-H "Content-Type: application/json" \\

  \-d '{"query":"{ listIngestionSources(input:{start:0,count:50}){ total ingestionSources { urn name type } } }"}'

## **4\. List a source's runs**

query runs(\$urn: String\!) {  
  ingestionSource(urn: \$urn) {  
    name  
    executions(start: 0, count: 20\) {  
      total  
      executionRequests {  
        urn  
        result { status startTimeMs durationMs }  
      }  
    }  
  }

}

Newest run first. total is how many runs are currently retained for that source, not how many have ever run — see **Retention**.

## **5\. Fetch the log for a run**

query log(\$urn: String\!) {  
  executionRequest(urn: \$urn) {  
    result {  
      status  
      startTimeMs  
      durationMs  
      report  
      structuredReport { type serializedValue }  
    }  
  }

}

report comes back as one long string. A failed run looks like this:

\~\~\~\~ Execution Summary \- RUN\_INGEST \~\~\~\~  
Execution finished with errors.  
{'exec\_id': 'd028afa2-b57b-4e3c-9916-f1f108fa8d6b',  
 'infos': \['... INFO: Starting execution for task with name=RUN\_INGEST',  
           "... INFO: Failed to execute 'datahub ingest', exit code 1",  
...  
ERROR {datahub.entrypoints:672} \- Command failed: Failed to configure the

source (my-postgres): \[Errno 13\] Permission denied: ...

## **Complete example**

Finds every failed run across all sources and writes each log to a file.

\#\!/usr/bin/env python3  
"""Dump the logs of all failed ingestion runs to ./ingestion-logs/."""

import json  
import os  
import pathlib  
import urllib.request

URL \= os.environ\["DATAHUB\_URL"\].rstrip("/") \+ "/api/graphql"  
HEADERS \= {  
    "Authorization": "Bearer " \+ os.environ\["DATAHUB\_TOKEN"\],  
    "Content-Type": "application/json",  
}

def gql(query, variables=None):  
    body \= json.dumps({"query": query, "variables": variables or {}}).encode()  
    req \= urllib.request.Request(URL, data=body, headers=HEADERS)  
    payload \= json.loads(urllib.request.urlopen(req, timeout=60).read())  
    if payload.get("errors"):  
        raise SystemExit(json.dumps(payload\["errors"\], indent=2))  
    return payload\["data"\]

SOURCES \= "{ listIngestionSources(input:{start:0,count:100}){ ingestionSources { urn name } } }"  
RUNS \= """  
query runs(\$urn: String\!) {  
  ingestionSource(urn: \$urn) {  
    executions(start: 0, count: 50\) {  
      executionRequests { urn result { status startTimeMs } }  
    }  
  }  
}  
"""  
LOG \= "query log(\$urn: String\!) { executionRequest(urn: \$urn) { result { report } } }"

outdir \= pathlib.Path("ingestion-logs")  
outdir.mkdir(exist\_ok=True)

for source in gql(SOURCES)\["listIngestionSources"\]\["ingestionSources"\]:  
    detail \= gql(RUNS, {"urn": source\["urn"\]})\["ingestionSource"\]  
    if not detail:  
        print(f"{source\['name'\]}: not visible with this token, skipping")  
        continue

    for run in detail\["executions"\]\["executionRequests"\]:  
        result \= run\["result"\] or {}  
        if result.get("status") \!= "FAILURE":  
            continue

        run\_id \= run\["urn"\].rsplit(":", 1)\[-1\]  
        report \= (gql(LOG, {"urn": run\["urn"\]})\["executionRequest"\]\["result"\] or {}).get("report") or ""  
        path \= outdir / f"{source\['name'\]}-{result.get('startTimeMs')}-{run\_id}.log"  
        path.write\_text(report)

        print(f"wrote {path} ({len(report)} chars)")

Run it with:

DATAHUB\_URL="https\://\<your-datahub-url\>" DATAHUB\_TOKEN="\<token\>" python3 dump\_failed\_logs.py

## **Retention**

Run history is pruned by the built-in datahub-gc ingestion source. How far back it reaches is not a guarantee, so do not design around a particular window — read executions.total and the run timestamps to see what is actually there.

One thing worth knowing: history is \*not\* discarded just because the next run succeeded. A failed run stays queryable for a while after later runs pass.

If you need a failed run's log for longer than the short term, copy it out rather than relying on DataHub to hold it. The script above is enough to do that on a schedule.

## **Limits worth knowing**

The executor caps what it stores in report, so treat it as a report rather than a verbatim log stream:

* the last **2000 lines** — earlier output is dropped and the text is prefixed with \[earlier logs truncated...\]  
* **4 KB per line** — longer lines are truncated individually  
* about **3.8 MB** in total

structuredReport is not always populated; it was empty on some failed runs and 6 KB on others. Read report and treat structuredReport as a bonus.

A run that hangs without reaching a terminal status never writes a result at all, so it has no report and no status to poll — see [datahub\#17397](https://github.com/datahub-project/datahub/issues/17397). Detecting those needs a check for runs that have been RUNNING too long.

