import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point

def search_shapefile(filepath, search_term):
    """
    Reads a shapefile and searches for a specific term in all string columns.
    
    Args:
        filepath (str): The path to the .shp file.
        search_term (str): The term to search for (case-insensitive).
    """
    print(f"--- Searching in file: {filepath} ---")
    try:
        # Read the shapefile
        gdf = gpd.read_file(filepath)
        
        print(f"Shapefile '{filepath}' successfully read!")
        
        found = False
        
        # Iterate over each column in the GeoDataFrame
        for column in gdf.columns:
            # Check if the column's data type is a string (object)
            if gdf[column].dtype == 'object':
                # Use a boolean mask to find rows where the column contains the search term
                matches = gdf[gdf[column].str.contains(search_term, case=False, na=False)]
                
                # If matches are found, print the results
                if not matches.empty:
                    print(f"Found '{search_term}' in the '{column}' column:")
                    print("-" * 30)
                    print(matches)
                    print("\n" + "=" * 30 + "\n")
                    found = True
        
        if not found:
            print(f"The word '{search_term}' was not found in any string column of the shapefile.")
        
        return found, gdf
        
    except FileNotFoundError:
        print(f"Error: The file '{filepath}' was not found.")
        return False, None
    except Exception as e:
        print(f"An error occurred: {e}")
        return False, None

if __name__ == "__main__":
    # The list of shapefiles to search
    shapefile_names = [
        "pri_admbnda_adm0_2019.shp",
        "pri_admbnda_adm1_2019.shp"
    ]
    
    # The term to search for
    search_term = "Ponce"
    
    # Loop through the list of files and perform the search
    for file_name in shapefile_names:
        found, gdf = search_shapefile(file_name, search_term)
        
        # If the search term was found in this file, plot the geometry
        if found:
            print(f"\nPlotting all geometries from {file_name}...")
            
            # Create a figure and axes object to set the plot size
            fig, ax = plt.subplots(figsize=(24, 18))  # Set the figure size to be 3x bigger
            
            # Plot the entire GeoDataFrame on the axes
            gdf.plot(ax=ax, edgecolor='black', linewidth=0.5, color='lightblue')
            
            # Set the title and labels
            ax.set_title(f"Administrative Boundaries from\n{file_name}", fontsize=20)
            ax.set_xlabel("Longitude")
            ax.set_ylabel("Latitude")

            # Create a point and annotation for the hover effect
            point = ax.scatter([], [], s=100, color='red')
            annot = ax.annotate("", xy=(0,0), xytext=(20, 20), textcoords="offset points",
                                bbox=dict(boxstyle="round", fc="w"),
                                arrowprops=dict(arrowstyle="->"))
            annot.set_visible(False)

            # Function to handle mouse movement
            def on_hover(event):
                vis = annot.get_visible()
                if event.inaxes == ax:
                    # Check if the mouse is over any polygon
                    for idx, row in gdf.iterrows():
                        if row['geometry'].contains(Point(event.xdata, event.ydata)):
                            # If it is, update the annotation with the municipality name
                            municipality_name = row['ADM1_ES']
                            annot.xy = (event.xdata, event.ydata)
                            annot.set_text(municipality_name)
                            annot.set_visible(True)
                            fig.canvas.draw_idle()
                            return
                
                # If not hovering over a polygon, hide the annotation
                if vis:
                    annot.set_visible(False)
                    fig.canvas.draw_idle()

            # Connect the mouse movement event to the on_hover function
            fig.canvas.mpl_connect('motion_notify_event', on_hover)
            
            plt.show()
            print("Plot display complete.")
