from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import http.client
import requests
import ssl
from urllib.parse import urlparse
from datetime import datetime

app = Flask(__name__)
status_report = {
    "threshold": None,
    "exceeding_temperatures": False,
    "last_check": None,
    "used_flux_query": None
}

# Konfiguration
GRAPHDB_BASE_URL = "http://134.147.216.21:7200"
REPOSITORY_ID = "ymodule" 
INFLUX_TOKEN = "1JQJJxOJvS99fv780_5VhDk1QuVYX7nYo-AmnVyY_iX7tNmQGqrs-LPQ469QKJoChW6kivrCAj9EhjlIFVJoGA=="


def perform_sparql_query(query: str) -> list[dict]:
    url = f"{GRAPHDB_BASE_URL}/repositories/{REPOSITORY_ID}"
    headers = {'Accept': 'application/sparql-results+json'}
    try:
        response = requests.get(url, headers=headers, params={'query': query})
        response.raise_for_status()
        bindings = response.json().get("results", {}).get("bindings", [])
        return [{k: v["value"] for k, v in b.items()} for b in bindings]
    except Exception as e:
        print(f"SPARQL query failed: {e}")
        return []


def query_temperature_threshold() -> float:
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX beo: <https://pi.pauwel.be/voc/buildingelement#>
    SELECT ?key ?value
    WHERE {
        ?submodel a <https://admin-shell.io/aas/3/0/Submodel>.
        ?submodel <https://admin-shell.io/aas/3/0/Submodel/submodelElements> ?ifcElements.
        ?ifcElements <https://admin-shell.io/aas/3/0/SubmodelElementList/value> ?element.
        ?element a beo:BuildingElement.
        ?element <https://admin-shell.io/aas/3/0/SubmodelElementList/value> ?aasProperties.
        ?aasProperties <https://admin-shell.io/aas/3/0/Property/value> ?value.
        ?aasProperties rdfs:label ?key.
        FILTER (contains(?key,"heatThreshold"))
    } LIMIT 1
    """
    results = perform_sparql_query(query)
    if results:
        try:
            print("[DTService] Threshold determined: " + (results[0]["value"]))
            return float(results[0]["value"])
        except ValueError:
            print("Warnung: Threshold nicht konvertierbar")
    return 80.0


def query_flux_query() -> tuple[str, str]:
    query = """
    PREFIX tsd: <https://rub-informatik-im-bauwesen.github.io/tsd#>
    SELECT ?endpoint ?query 
    WHERE {
        ?entity tsd:endpoint ?endpoint.
        ?entity tsd:query ?query.
    } LIMIT 1
    """
    results = perform_sparql_query(query)
    if results:
        endpoint = results[0].get("endpoint", "")
        query_str = results[0].get("query", "")
        print("[DTService] Endpoint determined: " + endpoint)
        print("[DTService] Query determined: " + query_str)
        return endpoint, query_str
    return "", ""


def check_influx_for_threshold(threshold: float, flux_template: str, influx_host: str) -> bool:
    if "__THRESHOLD__" not in flux_template:
        print("Flux-Query enthält keinen Platzhalter __THRESHOLD__")
        return False

    flux_query = flux_template.replace("__THRESHOLD__", str(threshold))
    print("[DTService] Threshold inserted into flux query: " + flux_query)

    parsed = urlparse(f"https://{influx_host}" if not influx_host.startswith("http") else influx_host)
    hostname = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path + ("?" + parsed.query if parsed.query else "")

    conn = http.client.HTTPSConnection(hostname, port, context=ssl._create_unverified_context())

    headers = {
        'Content-Type': "application/vnd.flux",
        'Authorization': f"Bearer {INFLUX_TOKEN}",
        'Accept': "*/*"
    }

    try:
        conn.request("POST", path, flux_query, headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        conn.close()
        print("[DTService] " + data)
        return "_value" in data
    except Exception as e:
        print(f"InfluxDB-Abfrage fehlgeschlagen: {e}")
        return False
    finally:
        conn.close()


def scheduled_check():
    threshold = query_temperature_threshold()
    influx_host, flux_query = query_flux_query()
    result = check_influx_for_threshold(threshold, flux_query, influx_host)

    status_report.update({
        "threshold": threshold,
        "exceeding_temperatures": result,
        "last_check": datetime.utcnow().isoformat() + "Z",
        "used_flux_query": flux_query,
        "influx_host": influx_host
    })

    print(f"[{status_report['last_check']}] Threshold={threshold} @ {influx_host} → Exceeded={result}")

@app.route('/status', methods=['GET'])
def get_status():
    return jsonify(status_report)


# Scheduler initialisieren
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_check, 'interval', minutes=2)
scheduler.start()

# Initialer Aufruf beim Start
scheduled_check()

if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
