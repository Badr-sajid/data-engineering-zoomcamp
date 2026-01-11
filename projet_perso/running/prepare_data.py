import re
from datetime import datetime
from icalendar import Calendar
from pathlib import Path
import json
import click

def parse_description(raw_text):
    """
    Parse la description brute pour extraire Description, Laps et PBs.
    """
    if not raw_text:
        return {}

    # 1. Nettoyage initial : 
    # Le format .ics coupe parfois les lignes avec "\n " (saut de ligne + espace).
    # On normalise tout ça pour avoir un texte fluide.
    clean_text = raw_text.replace('\n ', '').replace('\r', '')

    # Dictionnaire pour stocker les résultats
    data = {
        "Description": None,
        "Laps": [],
        "PBs": {}
    }

    # 2. Définition des REGEX pour capturer les blocs de texte
    # (?s) active le mode "DOTALL" (le point . capture aussi les sauts de ligne)
    # (?=...) est un "lookahead" pour s'arrêter AVANT le prochain marqueur (Emoji ou fin)

    # --- Extraction du bloc Description ---
    desc_match = re.search(r"📋 Description:\s*(.*?)\s*(?=♻️|🏆|📲|$)", clean_text, re.DOTALL)
    if desc_match:
        data["Description"] = desc_match.group(1).strip()

    # --- Extraction et Parsing des Laps (Tours) ---
    laps_block_match = re.search(r"♻️ Laps:\s*(.*?)\s*(?=🏆|📲|$)", clean_text, re.DOTALL)
    if laps_block_match:
        laps_text = laps_block_match.group(1)
        # On cherche chaque ligne de lap : "12.00 km @ 6:09 /km"
        # Pattern : (Distance) km @ (Allure) /km
        laps_items = re.findall(r"([\d\.]+)\s*km\s*@\s*(\d+:\d+)\s*/km", laps_text)

        for dist, pace in laps_items:
            data["Laps"].append({
                "distance_km": float(dist),
                "pace": pace
            })

    # --- Extraction et Parsing des PBs (Records) ---
    pbs_block_match = re.search(r"🏆 PBs:\s*(.*?)\s*(?=📲|$)", clean_text, re.DOTALL)
    if pbs_block_match:
        pbs_text = pbs_block_match.group(1)
        # Pattern : (Nom): (Temps) ex: "5km: 30:17"
        pbs_items = re.findall(r"(.+?):\s*(\d{1,2}:\d{2}(?::\d{2})?)", pbs_text)

        for name, time in pbs_items:
            # Nettoyage du nom (ex: enlever les sauts de ligne)
            clean_name = name.strip()
            data["PBs"][clean_name] = time

    return data

def create_events_data(calendar):
    events_data = []
    for event in calendar.events:
        if '🏃' in event.get('SUMMARY'):
            summary = str(event.get('SUMMARY'))
            titre = re.sub(r'[^\w\s\.\-\(\)]', '', summary).strip()
            date = event.get('DTSTART').dt.strftime('%d/%m/%Y')
            parsed_description = parse_description(event.get('DESCRIPTION'))
            if parsed_description.get('Description'):
                actual_event = {
                        'Titre': titre,
                        'date': date,
                        'Description': parsed_description.get('Description'),
                        'Laps': parsed_description.get('Laps'),
                        'PBs': parsed_description.get('PBs')
                        }
                events_data.append(actual_event)
    return events_data

def save_events_to_json(events_data, OUTPUT_FILE):
    events_data.sort(key=lambda x: datetime.strptime(x['date'], '%d/%m/%Y'))
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(events_data, f, indent=4, ensure_ascii=False, default=str)


@click.command()
@click.option("--ics_file", help="Path to the ICS file")
@click.option("--json_file", help="Path to the JSON output file")
def run (ics_file, json_file):

    calendar = Calendar.from_ical(Path(ics_file).read_bytes())

    events_data = create_events_data(calendar)
    save_events_to_json(events_data, json_file)

if __name__ == "__main__":
    run()