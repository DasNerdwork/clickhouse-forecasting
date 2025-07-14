import os
import sys
import time
import pandas as pd
from prophet import Prophet
from clickhouse_connect import get_client
import logging
from logging.handlers import RotatingFileHandler
import argparse

# FUNKTION:
# Dieses Skript erstellt Prognosen für Zeitreihen-Daten in einer ClickHouse-Datenbank mithilfe des Prophet-Modells. 
# Es verarbeitet Tabellen, die Zeitstempeldaten enthalten, erstellt neue Prognosetabellen und fügt prognostizierte Werte ein.
#
# VERWENDUNG:
# python forecast_script.py <db_name> <interval> [<specific_tables>] [--only-future]
#
# PARAMETER:
# - db_name (Pflicht): Der Name der ClickHouse-Datenbank, die verarbeitet werden soll.
# - interval (Pflicht): Die Anzahl an Tagen, die für die zukünftige Prognose erstellt werden sollen.
# - specific_tables (Optional): Eine durch Kommata getrennte Liste von Tabellennamen, die explizit bearbeitet werden sollen. Wenn nicht angegeben, werden alle Tabellen verarbeitet.
# - only-future (Optional): Eine Flag die entscheidet ob prognostizierte Daten ausschließlich für zukünftige Daten generiert werden sollen.
#
# FEHLERBEHANDLUNG:
# - Alle nicht abgefangenen Fehler werden im forecast.log protokolliert.
# - Das Skript unterstützt eine rotierende Logdatei, die maximal 50 MB groß ist und bis zu 3 Rotationen speichert.
#
# VORAUSSETZUNGEN:
# Folgende Umgebungsvariablen müssen für die Verbindung zu ClickHouse gesetzt sein:
#    - 'CLICKHOUSE_HOST': Hostname oder IP-Adresse des ClickHouse-Servers (Standard: localhost)
#    - 'CLICKHOUSE_PORT': Port des ClickHouse-Servers (Standard: 8123)
#    - 'CLICKHOUSE_USERNAME': Benutzername für die ClickHouse-Authentifizierung
#    - 'CLICKHOUSE_PASSWORD': Passwort für die ClickHouse-Authentifizierung
#
# ABHÄNGIGKEITEN (über pip installierbar):
#    - 'pandas': Zur Verarbeitung von DataFrames und Zeitreihendaten
#    - 'prophet': Für die Berechnung der Datenprognosen
#    - 'clickhouse-connect': Für die Verbindung zur ClickHouse-Datenbank
#
# WICHTIGE HINWEISE:
# 1. Dieses Skript benötigt Python 3.7 oder höher.
# 2. Die Tabellen in der angegebenen Datenbank müssen eine 'ds'-Spalte mit Datumswerten und mindestens eine weitere numerische Spalte enthalten.
# 3. Für jede verarbeitete Tabelle wird eine neue Tabelle erstellt, die die Prognosedaten speichert. Die Tabellen haben das Format 'bucket_forecast_<table_name>' und enthalten folgende Spalten:
#    - 'date': Datum
#    Und pro vorhandenem numerischen Tabellenwert aus der Originaltabelle jeweils:
#    - 'yhat': Prognostizierter Mittelwert
#    - 'yhat_min': Untere Grenze der Prognose
#    - 'yhat_max': Obere Grenze der Prognose
# 4. Falls bereits eine Prognosetabelle für eine Tabelle existiert, wird diese gelöscht und neu erstellt.
# 5. Zudem können Tabellenspalten mit spezifischen Datentypen übersprungen werden indem die Variable SKIP_DATA_TYPES angepasst wird
#
# Beispielaufrufe:
# python forecast_script.py forecast_db 7
# python forecast_script.py forecast_db 30 bucket_bounce_rate,bucket_order_items
# python forecast_script.py forecast_db 14 bucket_bounce_rate,bucket_order_items --only-future
# python forecast_script.py forecast_db 30 bucket_bounce_rate --only-future

# Rotierendes Logging konfigurieren & Log säubern
handler = RotatingFileHandler('forecast.log', maxBytes=50 * 1024 * 1024, backupCount=3) # Maximalegröße des Logs auf 50 MB und max. 3 Rotationsbackups
handler.setFormatter(logging.Formatter('[%(asctime)s] [%(levelname)s]: %(message)s', datefmt='%d.%m.%Y %H:%M:%S'))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logging.getLogger('prophet').setLevel(logging.WARNING)
logging.getLogger('cmdstanpy').propagate = False
logging.getLogger('matplotlib').propagate = False

# Globale Variablen initialisieren
start_time = time.time()
SKIP_DATA_TYPES = {"String", "Text", "Enum", "Boolean", "Blob", "Binary", "Array", "JSON", "UUID"} # Datentypen von Tabellenspalten die in der Prognose ignoriert werden sollen
successful_tables = []
new_tables = []
updated_tables = []
failed_tables = []
skipped_tables = []

# Nicht abgefangene Fehler ebenfalls ins Log schreiben 
def log_exception(exc_type, exc_value, exc_traceback):
    logging.error("Beim Ausführen des Skripts ist folgender Fehler aufgetreten:", exc_info=(exc_type, exc_value, exc_traceback))
    sys.__excepthook__(exc_type, exc_value, exc_traceback)
sys.excepthook = log_exception

# ClickHouse Datenbankverbindung via Umgebungsvariablen herstellen
def get_clickhouse_connection():
    try:
        host = os.getenv("CLICKHOUSE_HOST", "localhost") # Standardhost = localhost
        port = int(os.getenv("CLICKHOUSE_PORT", 8123)) # Standardport = 8123
        username = os.getenv("CLICKHOUSE_USERNAME")
        password = os.getenv("CLICKHOUSE_PASSWORD")
        database = db_name
        client = get_client(host=host, port=port, username=username, password=password, database=database)
        logging.info("Verbindung zur ClickHouse Datenbank erfolgreich hergestellt")
        return client
    except Exception as e:
        logging.error(f"Fehler beim Verbinden mit der ClickHouse Datenbank: {e}")
        sys.exit(1)

# Funktion um alle Tabellennamen der Datenbank zu erhalten
def get_tables(client, db_name):
    query = f"SHOW TABLES FROM {db_name}"
    logging.debug(f"Executing query: {query}")
    tables = client.query(query).result_set
    return [table[0] for table in tables]

# Funktion um alle Spalten der Tabellen zu erhalten
# Eine Clickhouse Tabelle besteht aus 7 Eigenschaften name, type, default_type, default_expression, comment, codec_expression und ttl_expression
# Wir benutzen hier nur die ersten beiden, also den Namen und den DataType
def get_columns_and_types(client, db_name, table_name):
    query = f"DESCRIBE TABLE {db_name}.{table_name}"
    columns_info = client.query(query).result_set
    columns = []
    types = []
    for column, type_, *rest in columns_info: # *rest notwendig damit die anderen 5 Eigenschaften ignoriert werden
        if column != "date" and not any(skip_type in type_ for skip_type in SKIP_DATA_TYPES):
            columns.append(column)
            types.append(type_)
        elif column != "date":
            logging.info(f"Überspringe Spalte mit ungültigem Datentyp: {column}: {type_}")
    return columns, types

# Funktion zum Erstellen der Prognosetabellen
def create_forecast_table(client, db_name, table_name, columns, types):
    if table_name.strip().startswith("bucket_"):
        forecast_table_name = f"{db_name}.bucket_forecast_{table_name[7:]}"  # "bucket_" Präfix bei mögl. Dopplung entfernen
    else:
        forecast_table_name = f"{db_name}.bucket_forecast_{table_name}"
    short_table_name = forecast_table_name.split(".", 1)[-1] # Abkürzung fürs Logging
    
    # Bereits existierende Prognosetabelle löschen
    query_check = f"EXISTS TABLE {forecast_table_name}"
    exists = client.query(query_check).result_set[0][0] == 1
    if exists:
        logging.info(f"Lösche {short_table_name} da bereits existent")
        client.query(f"DROP TABLE {forecast_table_name}")

    # Prognosewerte yhat, yhat_upper und yhat_lower zuweisen
    columns_definitions = ", ".join([f"{col} {typ}, {col}_min {typ}, {col}_max {typ}" for col, typ in zip(columns, types)])

    # (Neu)Erstellung der Grundstruktur Prognosetabelle
    query_create = f"""
    CREATE TABLE {forecast_table_name} (
        date Date,
        {columns_definitions}
    ) ENGINE = MergeTree()
    ORDER BY date;
    """
    client.query(query_create)
    if not exists:
        logging.info(f"Tabelle {short_table_name} erstellt")
        new_tables.append(table_name)
    else:
        logging.info(f"Tabelle {short_table_name} neu erstellt")
        updated_tables.append(table_name)

# Generieren und Einfügen von prognostizierte Daten für bestimmte Spalten
def forecast_table(client, db_name, table_name, interval, only_future):
    columns, types = get_columns_and_types(client, db_name, table_name)
    forecast_table_name = f"{db_name}.bucket_forecast_{table_name[7:]}"
    query = f"SELECT date, {', '.join(columns)} FROM {db_name}.{table_name} ORDER BY date"
    data = pd.DataFrame(client.query(query).result_set, columns=["date"] + columns)

    if data.empty:
        logging.warning(f"Fehlende Daten in {table_name}")
        failed_tables.append(table_name)
        return
    
    data['date'] = pd.to_datetime(data['date'], format='%Y-%m-%d')
    last_known_date = data['date'].max()  # Letztes vorhandenes Datum

    forecast_data = {}
    for col in columns:
        try:
            model = Prophet()
            df = data[["date", col]].rename(columns={"date": "ds", col: "y"})
            model.fit(df)
            future = model.make_future_dataframe(periods=interval)
            if only_future:
                future = future[future["ds"] > last_known_date]  # Entferne bekannte Daten
            forecast = model.predict(future)
            # Vorbereiten der Daten für den Insert
            export_data = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']]
            export_data = export_data.copy()
            export_data.loc[:, 'ds'] = pd.to_datetime(export_data['ds'], errors='coerce').dt.date

            # Erstellen der neuen Spalten: *_min, *_max
            export_data.loc[:, f'{col}_min'] = export_data['yhat_lower']
            export_data.loc[:, f'{col}_max'] = export_data['yhat_upper']

            # Für jedes Datum die Werte im Dictionary sammeln
            for _, row in export_data.iterrows():
                date = row['ds']
                if date not in forecast_data:
                    forecast_data[date] = {'date': date}
                forecast_data[date][col] = row['yhat']
                forecast_data[date][f'{col}_min'] = row[f'{col}_min']
                forecast_data[date][f'{col}_max'] = row[f'{col}_max']

        except Exception as e:
            failed_tables.append(table_name)
            logging.error(f"Fehler beim Einfügen der prognostizierten Prophet-Daten für {col} in {table_name}: {e}")

    # Einfügen der gesammelten prognostizierten Daten
    if forecast_data:
        # Baue die VALUES für den Insert-Befehl
        query_insert = f"""
        INSERT INTO {forecast_table_name} (date, {', '.join(columns)}, {', '.join([f'{col}_min' for col in columns])}, {', '.join([f'{col}_max' for col in columns])})
        VALUES
        """
        query_values = ", ".join([
            f"('{data['date']}', {', '.join([str(data[col]) if col in data else 'NULL' for col in columns])}, "
            f"{', '.join([str(data[f'{col}_min']) if f'{col}_min' in data else 'NULL' for col in columns])}, "
            f"{', '.join([str(data[f'{col}_max']) if f'{col}_max' in data else 'NULL' for col in columns])})"
            for data in forecast_data.values()
        ])
        client.query(query_insert + query_values)  # Insert in die Tabelle

        logging.info(f"Daten für {forecast_table_name} eingefügt")

    successful_tables.append(table_name)

# Hauptlogik des Skripts
def main(db_name, interval, specific_tables=None, only_future=False):
    client = get_clickhouse_connection()

    if specific_tables and isinstance(specific_tables, str):
        tables = specific_tables.split(',')  # Splitte nur, wenn es ein String ist
    else:
        tables = get_tables(client, db_name)


    for table in tables:
        # Falls explizit Tabellen bei der Skriptausführung angegeben werden, bearbeite nur diese
        if specific_tables and table not in specific_tables:
            continue
        
        if table.startswith("bucket_forecast_"):
            skipped_tables.append(table)
            continue
        
        logging.info(f"Starte Verarbeitung von {db_name}.{table}")
        # Prognosetabelle erstellen und Daten verarbeiten
        columns, types = get_columns_and_types(client, db_name, table)
        create_forecast_table(client, db_name, table, columns, types)
        forecast_table(client, db_name, table, interval, only_future)

    duration = time.time() - start_time # Berechnung der Laufzeit
    logging.info(f"Prognoseverfahren erfolgreich beendet. Dauer: {duration:.2f} Sekunden")
    logging.info(f"Tabelleninformationen: Erfolgreich: {len(successful_tables) + len(skipped_tables)}, Neu: {len(new_tables)}, Aktualisiert: {len(updated_tables)}, Übersprungen: {len(skipped_tables) - len(failed_tables)}, Fehlerhaft: {len(failed_tables)}")
    logging.info("-----------------------------------------------------------------------------------------------")

if __name__ == "__main__":
    # Spezifizierung der Eingabeparameter mit argparse
    parser = argparse.ArgumentParser(
        description="Prognose für ClickHouse-Tabellen erstellen.",
        usage="python forecast_script.py <db_name> <interval> [<specific_tables>] [--only-future]")
    parser.add_argument("db_name", help="Name der ClickHouse-Datenbank")
    parser.add_argument("interval", type=int, help="Anzahl an Tagen für die Prognose")
    parser.add_argument("specific_tables", nargs="?", default=None, help="Komma-separierte Liste spezifischer Tabellen")
    parser.add_argument("--only-future", action="store_true", help="Nur zukünftige Daten prognostizieren")
    args = parser.parse_args()

    # Zuweisung der Argumentwerte
    db_name = args.db_name
    interval = args.interval
    if args.specific_tables:
        specific_tables = args.specific_tables
    else:
        specific_tables = None
    only_future = args.only_future

    # Aufruf der Hauptfunktion
    main(db_name, interval, specific_tables, only_future)