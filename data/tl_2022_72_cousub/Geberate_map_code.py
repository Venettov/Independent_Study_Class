import geopandas as gpd
import json
import os

def create_geojson_from_shapefile(filepath, geojson_filename="pr_municipalities.geojson"):
    """
    Reads a shapefile, extracts municipality data, converts it to GeoJSON, and saves it to a file.
    
    Args:
        filepath (str): The path to the .shp file.
        geojson_filename (str): The name of the output GeoJSON file.
    
    Returns:
        str: The name of the created GeoJSON file, or None if an error occurs.
    """
    try:
        # Read the shapefile into a GeoDataFrame
        gdf = gpd.read_file(filepath)

        # Ensure the GeoDataFrame has the correct CRS (WGS84 is standard for web maps)
        if gdf.crs.to_string() != 'EPSG:4326':
            gdf = gdf.to_crs(epsg=4326)

        # Filter for the relevant columns and convert to GeoJSON
        gdf = gdf[['ADM1_ES', 'geometry']]
        geojson_data = json.loads(gdf.to_json())

        # Rename the 'ADM1_ES' property to 'name'
        for feature in geojson_data['features']:
            if 'ADM1_ES' in feature['properties']:
                feature['properties']['name'] = feature['properties'].pop('ADM1_ES')

        with open(geojson_filename, "w", encoding="utf-8") as f:
            json.dump(geojson_data, f, indent=2)

        print(f"Successfully generated {geojson_filename}.")
        return geojson_filename

    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def create_html_map_file(geojson_filename, output_filename="maps.html"):
    """
    Generates a complete HTML file with an embedded map that loads a GeoJSON file.
    """
    if geojson_filename is None:
        print("Cannot create HTML file without a GeoJSON data file.")
        return

    # Use a direct fetch call as it is the standard and should work
    # Note: Double curly braces {{ and }} are used to escape the JavaScript code inside the f-string
    html_template = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Puerto Rico Municipalities</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
        integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
        crossorigin=""/>
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
        integrity="sha256-20n69z7S+y9oRbywbydY+a/oYvSjY+yT5q4lQ4a4R28="
        crossorigin=""></script>
    <style>
        body {{
            font-family: 'Inter', sans-serif;
            background-color: #f3f4f6;
        }}
        #map {{
            height: 85vh;
            width: 100%;
            border-radius: 1rem;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1), 0 4px 6px -2px rgba(0, 0, 0, 0.05);
        }}
        .leaflet-tooltip {{
            background: rgba(255, 255, 255, 0.9);
            border-color: #333;
            color: #333;
            font-size: 14px;
            font-weight: bold;
            box-shadow: 0 2px 5px rgba(0,0,0,0.2);
            padding: 5px 10px;
        }}
    </style>
</head>
<body class="flex items-center justify-center p-8 min-h-screen">
    <div class="container mx-auto max-w-7xl">
        <div class="bg-white rounded-xl p-6 shadow-lg">
            <h1 class="text-3xl font-bold text-center text-gray-800 mb-4">Puerto Rico Municipalities</h1>
            <p class="text-center text-gray-600 mb-6">Hover over a municipality to see its name.</p>
            <div id="map" class="rounded-lg"></div>
        </div>
    </div>

    <script>
        document.addEventListener('DOMContentLoaded', function() {{
            const map = L.map('map').fitBounds([
                [17.85, -67.45], 
                [18.55, -65.25]
            ]);

            L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
            }}).addTo(map);

            const geojsonUrl = 'https://raw.githubusercontent.com/venettov/Independent_Study_Class/main/data/tl_2022_72_cousub/pr_municipalities.geojson';

            fetch(geojsonUrl)
                .then(response => {{
                    if (!response.ok) {{
                        throw new Error(`HTTP error! Status: ${{response.status}}`);
                    }}
                    return response.json();
                }})
                .then(prMunicipalities => {{
                    const geojsonLayer = L.geoJSON(prMunicipalities, {{
                        style: {{
                            fillColor: 'lightblue',
                            weight: 1.5,
                            opacity: 1,
                            color: 'black',
                            fillOpacity: 0.7
                        }},
                        onEachFeature: function(feature, layer) {{
                            if (feature.properties && feature.properties.name) {{
                                layer.bindTooltip(feature.properties.name, {{
                                    permanent: false,
                                    direction: 'center',
                                    className: 'leaflet-tooltip',
                                    offset: [0, 0]
                                }});
                            }}

                            layer.on({{
                                mouseover: function(e) {{
                                    e.target.setStyle({{
                                        weight: 3,
                                        color: '#666',
                                        dashArray: '',
                                        fillOpacity: 0.9
                                    }});
                                    if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {{
                                        e.target.bringToFront();
                                    }}
                                }},
                                mouseout: function(e) {{
                                    geojsonLayer.resetStyle(e.target);
                                }},
                                click: function(e) {{
                                    map.fitBounds(e.target.getBounds());
                                }}
                            }});
                        }}
                    }}).addTo(map);
                }})
                .catch(error => {{
                    console.error('Error loading the GeoJSON data:', error);
                }});
        }});
    </script>
</body>
</html>
"""

    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"Successfully generated {output_filename} with map data.")

if __name__ == "__main__":
    shapefile_path = "pri_admbnda_adm1_2019.shp"
    
    if not os.path.exists(shapefile_path):
        print(f"Error: The file '{shapefile_path}' was not found.")
        print("Please ensure the shapefile is in the same directory as this script.")
    else:
        geojson_output_file = create_geojson_from_shapefile(shapefile_path)
        create_html_map_file(geojson_output_file)
