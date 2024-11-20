import json
from weaviate import Client

# Create client using v3 syntax
client = Client(
    url="http://localhost:8080"
)

# First, let's check if the class already exists and delete it if it does
try:
    client.schema.delete_class("PhonkMusic")
except Exception:
    pass  # Class didn't exist, which is fine

# Create the class
class_obj = {
    "class": "PhonkMusic",
    "vectorizer": "text2vec-transformers"
}

client.schema.create_class(class_obj)

# Phonk music data
phonk_data = [
    {
        "Title": "MURDER IN MY MIND",
        "Artist": "KORDHELL",
        "Subgenre": "Drift Phonk",
        "Characteristics": "Heavy bass, aggressive 808s, dark atmospheric samples, car drifting aesthetic, features haunting vocal samples"
    },
    {
        "Title": "WHY NOT",
        "Artist": "GHOSTFACE PLAYA",
        "Subgenre": "Classic Phonk",
        "Characteristics": "Memphis rap samples, cowbell percussion, dark synths, heavy bass, features distinct southern rap elements"
    },
    {
        "Title": "SHADOW",
        "Artist": "DVRST",
        "Subgenre": "Drift Phonk",
        "Characteristics": "Fast-paced beats, aggressive bass, car engine samples, features intense drum patterns and dark atmosphere"
    },
    {
        "Title": "VENDETTA!",
        "Artist": "MUPP",
        "Subgenre": "House Phonk",
        "Characteristics": "House music influences, energetic rhythm, deep bass, features modern electronic elements with classic phonk sounds"
    },
    {
        "Title": "MIDNIGHT",
        "Artist": "PLAYAMANE",
        "Subgenre": "Classic Phonk",
        "Characteristics": "Traditional Memphis-style samples, heavy bass hits, eerie synths, features old school hip-hop elements"
    },
    {
        "Title": "CLOSE EYES",
        "Artist": "DVRST",
        "Subgenre": "Drift Phonk",
        "Characteristics": "High energy beats, aggressive 808s, racing aesthetic, features powerful synthesizers and intense drops"
    },
    {
        "Title": "PHONKY TOWN",
        "Artist": "PLAYAPHONK",
        "Subgenre": "Drift Phonk",
        "Characteristics": "Iconic cowbell pattern, heavy bass, car drift samples, features a memorable melody and aggressive rhythm"
    },
    {
        "Title": "TOKYO DRIFT",
        "Artist": "XAVIER WULF",
        "Subgenre": "Classic Phonk",
        "Characteristics": "Racing themed, Memphis rap influence, heavy bass, features car culture references and traditional phonk elements"
    },
    {
        "Title": "SAHARA",
        "Artist": "HENSONN",
        "Subgenre": "Modern Phonk",
        "Characteristics": "Middle Eastern samples, trap influences, heavy bass, features unique cultural fusion and modern production"
    },
    {
        "Title": "KERAUNOS",
        "Artist": "PLAYAPHONK",
        "Subgenre": "Drift Phonk",
        "Characteristics": "Intense bass drops, aggressive rhythm, racing aesthetic, features powerful drum patterns and dark atmosphere"
    },
    {
        "Title": "MELODY OF SILENCE",
        "Artist": "PHARMACIST",
        "Subgenre": "Atmospheric Phonk",
        "Characteristics": "Ethereal samples, spacey atmosphere, heavy bass, features ambient elements while maintaining phonk energy"
    },
    {
        "Title": "WAKE UP",
        "Artist": "MOONDEITY",
        "Subgenre": "Modern Phonk",
        "Characteristics": "Electronic influences, heavy synths, aggressive bass, features contemporary production with phonk elements"
    },
    {
        "Title": "CRASH OUT",
        "Artist": "KAITO SHOMA",
        "Subgenre": "Drift Phonk",
        "Characteristics": "Racing themed, intense drops, heavy percussion, features car culture aesthetic and aggressive sound design"
    },
    {
        "Title": "RARE",
        "Artist": "GROOVE",
        "Subgenre": "House Phonk",
        "Characteristics": "House music rhythm, phonk bass, modern production, features dance-oriented elements with phonk characteristics"
    },
    {
        "Title": "DEVIL EYES",
        "Artist": "ZODIVK",
        "Subgenre": "Dark Phonk",
        "Characteristics": "Horror-themed samples, intense bass, dark atmosphere, features ominous sounds and aggressive rhythm"
    }
]

# Save to data.json
with open('data.json', 'w') as f:
    json.dump(phonk_data, f, indent=2)

print("Reading data.json...")
with open('data.json', 'r') as f:
    data = json.load(f)

print(f"Importing {len(data)} records...")

# Import data using batch
with client.batch as batch:
    batch.batch_size = 100

    for i, d in enumerate(data):
        print(f"\nimporting datum: {i}")

        properties = {
            "title": d["Title"],
            "artist": d["Artist"],
            "subgenre": d["Subgenre"],
            "characteristics": d["Characteristics"]
        }

        client.batch.add_data_object(
            properties,
            "PhonkMusic"
        )
