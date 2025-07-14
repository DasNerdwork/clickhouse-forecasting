### Skript:
In diesen Repository befindet sich ein Skript zur Erstellung von Prognosen für Zeitreihen-Daten in einer ClickHouse-Datenbank mithilfe des Prophet-Modells. Es verarbeitet Tabellen, die Zeitstempeldaten enthalten, erstellt neue Prognosetabellen und fügt prognostizierte Werte ein.

#### Verwendung:
```bash
python forecast_script.py <db_name> <interval> [<specific_tables>] [--only-future]
```

#### Parameter:
- **db_name** (Pflicht): Der Name der ClickHouse-Datenbank, die verarbeitet werden soll.
- **interval** (Pflicht): Die Anzahl an Tagen, die für die zukünftige Prognose erstellt werden sollen.
- **specific_tables** (Optional): Eine durch Kommata getrennte Liste von Tabellennamen, die explizit bearbeitet werden sollen. Wenn nicht angegeben, werden alle Tabellen verarbeitet.
- **only-future** (Optional): Eine Flag die entscheidet ob prognostizierte Daten ausschließlich für zukünftige Daten generiert werden sollen.

#### Fehlerbehandlung:
- Alle nicht abgefangenen Fehler werden im forecast.log protokolliert.
- Das Skript unterstützt eine rotierende Logdatei, die maximal 50 MB groß ist und bis zu 3 Rotationen speichert.

#### Voraussetzungen:
Folgende Umgebungsvariablen müssen für die Verbindung zu ClickHouse gesetzt sein:
   - **CLICKHOUSE_HOST**: Hostname oder IP-Adresse des ClickHouse-Servers (Standard: localhost)
   - **CLICKHOUSE_PORT**: Port des ClickHouse-Servers (Standard: 8123)
   - **CLICKHOUSE_USERNAME**: Benutzername für die ClickHouse-Authentifizierung
   - **CLICKHOUSE_PASSWORD**: Passwort für die ClickHouse-Authentifizierung

#### Abhängigkeiten (über pip installierbar):
   - **pandas**: Zur Verarbeitung von DataFrames und Zeitreihendaten
   - **prophet**: Für die Berechnung der Datenprognosen
   - **clickhouse-connect**: Für die Verbindung zur ClickHouse-Datenbank

#### Wichtige Hinweise:
1. Dieses Skript benötigt **Python 3.7** oder höher.
2. Die Tabellen in der angegebenen Datenbank müssen eine 'date'-Spalte mit Datumswerten und *mindestens* eine weitere numerische Spalte (UInt32, Float, etc.) enthalten.
3. Für jede verarbeitete Tabelle wird eine neue Tabelle erstellt, die die Prognosedaten speichert. Die Tabellen haben das Format `bucket_forecast_<table_name>` und enthalten folgende Spalten:
   - **date**: Datum
   Und pro vorhandenem numerischen Tabellenwert aus der Originaltabelle jeweils:
   - **yhat**: Prognostizierter Mittelwert
   - **yhat_min**: Untere Grenze der Prognose
   - **yhat_max**: Obere Grenze der Prognose
4. Falls bereits eine Prognosetabelle für eine Tabelle existiert, wird diese gelöscht und neu erstellt.
5. Zudem können Tabellenspalten mit spezifischen Datentypen übersprungen werden indem die Variable **SKIP_DATA_TYPES** angepasst wird

Beispielaufrufe:
```bash
python forecast_script.py forecast_db 7
python forecast_script.py forecast_db 30 bucket_bounce_rate,bucket_order_items
python forecast_script.py forecast_db 14 bucket_bounce_rate,bucket_order_items --only-future
python forecast_script.py forecast_db 30 bucket_bounce_rate --only-future
```
