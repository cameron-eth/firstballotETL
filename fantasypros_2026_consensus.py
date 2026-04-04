#!/usr/bin/env python3
"""FantasyPros 2026 superflex rookie consensus input for the prospect model."""

from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Optional


CONSENSUS_SOURCE = "FantasyPros 2026 Dynasty Rookie Rankings - Superflex"


RAW_2026_FANTASYPROS_SUPERFLEX = """
1|Jeremiyah Love|RB1|1|1|1.0|0.0
2|Makai Lemon|WR1|2|4|2.6|0.6
3|Carnell Tate|WR2|2|5|3.2|1.0
4|Jordyn Tyson|WR3|2|8|4.0|1.2
5|K.C. Concepcion|WR4|5|10|7.0|1.8
6|Kenyon Sadiq|TE1|5|12|7.1|2.1
7|Denzel Boston|WR5|5|15|8.9|2.7
8|Jonah Coleman|RB2|2|15|9.6|2.9
9|Eli Stowers|TE2|6|16|10.4|3.0
10|Jadarian Price|RB3|4|18|10.5|3.6
11|Omar Cooper Jr.|WR6|5|17|11.0|3.8
12|Fernando Mendoza|QB1|7|17|11.4|2.9
13|Mike Washington Jr.|RB4|6|29|13.8|5.2
14|Elijah Sarratt|WR7|5|21|14.0|4.0
15|Emmett Johnson|RB5|9|22|15.3|3.5
16|Nicholas Singleton|RB6|11|21|16.3|3.3
17|Kaytron Allen|RB7|9|28|17.7|3.9
18|Chris Brazzell II|WR8|12|29|19.1|4.6
19|Chris Bell|WR9|13|40|19.6|6.9
20|Ty Simpson|QB2|16|28|21.4|3.3
21|Germie Bernard|WR10|13|28|21.5|4.3
22|Zachariah Branch|WR11|16|32|23.1|4.7
23|Demond Claiborne|RB8|22|37|27.3|4.6
24|Ja'Kobi Lane|WR12|18|37|27.8|5.5
25|Antonio Williams|WR13|16|41|29.2|7.5
26|Malachi Fields|WR14|17|46|29.2|7.8
27|J'Mari Taylor|RB9|22|53|30.6|8.5
28|Skyler Bell|WR15|13|47|31.7|8.3
29|Garrett Nussmeier|QB3|21|47|31.8|8.3
30|Adam Randall|RB10|23|59|31.9|8.8
31|Max Klare|TE3|17|51|32.3|9.0
32|Seth McGowan|RB11|23|47|32.8|5.8
33|Roman Hemby|RB12|24|50|34.8|7.4
34|Michael Trigg|TE4|23|61|37.4|9.9
35|Ted Hurst|WR16|22|64|39.0|10.7
36|Le'Veon Moss|RB13|25|59|39.3|9.0
37|Bryce Lance|WR17|26|62|40.5|9.6
38|Drew Allar|QB4|24|56|40.6|9.2
39|Justin Joly|TE5|25|85|42.3|14.0
40|Carson Beck|QB5|29|60|43.5|9.1
41|Deion Burks|WR18|27|65|42.5|10.2
42|Cade Klubnik|QB6|21|66|47.2|10.2
43|Jam Miller|RB14|32|94|47.2|14.2
44|Eric McAlister|WR19|26|66|46.7|10.7
45|Robert Henry Jr.|RB15|31|77|46.8|10.1
46|Cole Payton|QB7|34|62|48.4|8.3
47|Jaydn Ott|RB16|32|76|48.7|11.4
48|Jack Endries|TE6|38|84|53.0|12.2
49|Kevin Coleman Jr.|WR20|34|66|49.9|8.7
50|Tanner Koziol|TE7|29|93|56.4|15.5
51|Taylen Green|QB8|36|95|55.5|12.9
52|Barion Brown|WR21|42|92|57.9|12.0
53|C.J. Daniels|WR22|49|75|58.0|6.5
54|Kaelon Black|RB17|24|69|49.8|13.7
55|Joe Royer|TE8|36|92|58.3|14.9
56|Desmond Reid|RB18|32|64|52.4|7.8
57|De'Zhaun Stribling|WR23|47|71|59.9|6.5
58|Terion Stewart|RB19|38|71|54.7|8.3
59|Oscar Delp|TE9|27|98|65.6|19.5
60|Sam Roush|TE10|31|107|59.5|20.9
61|Reggie Virgil|WR24|36|78|60.3|14.3
62|Caleb Douglas|WR25|35|89|63.8|15.0
63|Brenen Thompson|WR26|40|74|58.9|9.5
64|Noah Whittington|RB20|47|65|56.3|4.8
65|Rahsul Faison|RB21|44|90|61.0|10.4
66|Diego Pavia|QB9|52|84|61.8|10.4
67|Josh Cameron|WR27|44|80|64.0|9.8
68|Aaron Anderson|WR28|55|75|64.8|6.6
69|Dean Connors|RB22|38|86|61.4|13.9
70|Dane Key|WR29|50|99|70.0|12.8
71|Jamal Haynes|RB23|56|80|63.5|6.4
72|TJ Harden|RB24|50|91|65.5|11.5
73|Sawyer Robertson|QB10|46|78|65.8|11.2
74|Eric Rivers|WR30|59|79|69.8|6.1
75|Chase Roberts|WR31|59|90|73.1|8.3
76|J. Michael Sturdivant|WR32|57|90|74.5|9.4
77|Eli Raridon|TE11|43|99|78.2|16.2
78|Chip Trayanum|RB25|61|84|72.6|7.8
79|Dallen Bentley|TE12|47|100|77.2|18.3
80|Jeff Caldwell|WR33|45|106|74.6|17.9
81|Tyren Montgomery|WR34|51|90|74.2|10.9
82|RJ Maryland|TE13|54|98|75.6|12.9
83|Lewis Bond|WR35|74|87|80.2|3.4
84|Eli Heidenreich|RB26|52|97|78.0|14.1
85|Dae'Quan Wright|TE14|40|100|74.9|24.2
86|Harrison Wallace III|WR36|74|93|83.6|5.0
87|Jalon Daniels|QB11|50|99|73.4|17.1
88|Rueben Owens|RB27|72|88|79.1|5.9
89|Cyrus Allen|WR37|63|89|80.9|7.2
90|Luke Altmyer|QB12|50|103|82.0|18.9
91|Nyck Harbor|WR38|75|96|87.6|5.7
92|CJ Donaldson|RB28|61|109|87.9|14.6
93|Miller Moss|QB13|38|82|66.8|18.0
94|Malik Benson|WR39|45|101|76.6|20.8
95|Joey Aguilar|QB14|63|105|83.8|14.2
96|Dillon Bell|WR40|81|111|93.2|8.9
97|Noah Thomas|WR41|84|96|91.0|3.3
98|Marlin Klein|TE15|52|100|84.2|17.5
99|Trebor Pena|WR42|88|99|92.4|3.2
100|John Michael Gyllenborg|TE16|71|110|91.6|12.8
101|Jack Velling|TE17|64|111|88.5|15.3
102|Josh Cuevas|TE18|77|94|86.2|7.0
103|L.J. Martin|RB29|51|95|80.5|17.3
104|Haynes King|QB15|49|52|50.5|1.5
105|Conner Weigman|QB16|43|88|73.0|21.2
106|Hank Beatty|WR43|89|100|93.2|3.6
107|Malachi Nelson|QB17|46|92|76.7|21.7
108|Al-Jay Henderson|RB30|53|93|78.3|18.0
109|Zavion Thomas|WR44|79|104|91.8|8.0
110|Kendrick Law|WR45|85|98|91.8|5.0
111|Malik Rutherford|WR46|89|105|95.2|5.0
112|Vinny Anthony II|WR47|78|101|92.8|7.9
113|Arch Manning|QB18|44|103|83.0|27.6
114|Behren Morton|QB19|67|98|83.3|12.7
115|Chris Hilton Jr.|WR48|78|107|95.6|9.5
116|Emmanuel Henderson Jr.|WR49|73|98|89.0|11.3
117|Kentrel Bullock|RB31|88|108|96.7|8.4
118|Lake McRee|TE19|72|109|90.5|18.5
""".strip()


def normalize_consensus_name(name: Optional[str]) -> str:
    """Normalize names for robust matching across pasted sources and database rows."""
    value = unicodedata.normalize("NFKD", (name or "").replace("’", "'"))
    value = value.encode("ascii", "ignore").decode("ascii")
    value = value.lower()
    value = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b\.?", "", value)
    return re.sub(r"[^a-z0-9]+", "", value)


def parse_position_rank(position_rank: str) -> tuple[str, Optional[int]]:
    match = re.fullmatch(r"([A-Z]+)(\d+)", position_rank.strip())
    if not match:
        raise ValueError(f"Invalid position rank: {position_rank}")
    return match.group(1), int(match.group(2))


def parse_2026_fantasypros_superflex() -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for raw_line in RAW_2026_FANTASYPROS_SUPERFLEX.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        overall_rank_str, name, position_rank, best_str, worst_str, avg_str, stddev_str = line.split("|")
        position, position_rank_num = parse_position_rank(position_rank)

        rows.append(
            {
                "name": name.strip(),
                "name_key": normalize_consensus_name(name),
                "position": position,
                "consensus_rank": int(overall_rank_str),
                "consensus_position_rank": position_rank_num,
                "consensus_best_rank": int(best_str),
                "consensus_worst_rank": int(worst_str),
                "consensus_avg_rank": round(float(avg_str), 2),
                "consensus_rank_stddev": round(float(stddev_str), 2),
                "consensus_source": CONSENSUS_SOURCE,
            }
        )
    return rows


def build_consensus_lookup() -> Dict[tuple[str, str], Dict[str, object]]:
    """Build a lookup keyed by normalized name and position."""
    return {
        (row["name_key"], row["position"]): row
        for row in parse_2026_fantasypros_superflex()
    }

