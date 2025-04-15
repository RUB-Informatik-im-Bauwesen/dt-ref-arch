from flask import Flask, jsonify
from apscheduler.schedulers.background import BackgroundScheduler
import http.client
import requests
import ssl
from urllib.parse import urlparse
from datetime import datetime, timedelta

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
START_HEAT = datetime.now()


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


def build_heat_output_payload(value):
    return {
        "idShort": "HeatOutput",
        "description": [
            {"language": "en", "text": "Represents the current heating power applied to the oven as a percentage of its maximum capacity."},
            {"language": "de", "text": "Gibt die aktuell auf den Ofen angewendete Heizleistung in Prozent der maximalen Leistung an."}
        ],
        "semanticId": {
            "type": "ModelReference",
            "keys": [{"type": "ConceptDescription", "value": "https://example.com/ids/cd/6032_2151_4052_2696"}]
        },
        "valueType": "xs:double",
        "value": str(value),
        "modelType": "Property"
    }

def update_aas_heat_output(value):
    """Updates the HeatOutput property of the AASX server to the given value."""
    url = "http://localhost:3000/submodels/aHR0cHM6Ly9leGFtcGxlLmNvbS9pZHMvc20vODA3NV85MDUxXzQwNTJfODY3Mg/submodel-elements/HeatOutput"
    payload = build_heat_output_payload(value)
    response = requests.put(url, json=payload)
    return response.status_code in (200, 204)

def build_operating_state_payload(state):
    return {
        "idShort": "OperatingState",
        "description": [
            {"language": "en", "text": "Current operating state of the oven, indicating whether it is off, heating, holding, cooling, or in error."},
            {"language": "de", "text": "Aktueller Betriebszustand des Ofens, der angibt, ob er ausgeschaltet ist, heizt, hält, kühlt oder einen Fehler aufweist."}
        ],
        "semanticId": {
            "type": "ModelReference",
            "keys": [{"type": "ConceptDescription", "value": "https://example.com/ids/cd/0410_1151_4052_6512"}]
        },
        "valueType": "xs:string",
        "value": str(state),
        "modelType": "Property"
    }

def update_aas_operating_state(state):
    """Updates the OperatingState property of the AASX server to the given state string."""
    url = "http://localhost:3000/submodels/aHR0cHM6Ly9leGFtcGxlLmNvbS9pZHMvc20vODA3NV85MDUxXzQwNTJfODY3Mg/submodel-elements/OperatingState"
    payload = build_operating_state_payload(state)
    response = requests.put(url, json=payload)
    return response.status_code in (200, 204)


def scheduled_check():
    threshold = query_temperature_threshold()
    influx_host, flux_query = query_flux_query()
    result = check_influx_for_threshold(threshold, flux_query, influx_host)

    if result:        
        update_aas_heat_output(.4)              # percentage
    else:        
        update_aas_heat_output(.75)             # percentage

    current_time = datetime.now()
    end_time = START_HEAT + timedelta(hours=4)

    if START_HEAT <= current_time <= end_time:
        update_aas_operating_state("heating")      # within heating period
    else:
        update_aas_operating_state("cooldown")  # outside heating period
        update_aas_heat_output(.0)               # percentage

    status_report.update({
        "threshold": threshold,
        "exceeding_temperatures": result,
        "last_check": datetime.utcnow().isoformat() + "Z",
        "used_flux_query": flux_query,
        "influx_host": influx_host
    })

    print(f"[{status_report['last_check']}] Threshold={threshold} @ {influx_host} → Exceeded={result}")

def start_heat_treatment():
    update_aas_heat_output(1.0)
    update_aas_operating_state("heating")
    global START_HEAT
    START_HEAT = datetime.now()
    
    
@app.route('/status', methods=['GET'])
def get_status():
    return jsonify(status_report)


# Scheduler initialisieren
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_check, 'interval', minutes=2)
scheduler.start()

# Initialer Aufruf beim Start
start_heat_treatment()
scheduled_check()


if __name__ == '__main__':
    app.run(debug=True, use_reloader=False)
