BASE_CONFIG = {
    "version": "v1",
    "config": {
        "mapState": {
            "latitude": -27.5,
            "longitude": 153.0,
            "zoom": 7,
            "bearing": 0,
            "pitch": 0
        },
        "mapStyle": {
            # Default base map: SATELLITE
            "styleType": "satellite",
            "topLayerGroups": {},
            "visibleLayerGroups": {
                "label": True, "road": True, "border": False, "building": True, "water": True, "land": True
            }
        },
        "visState": {
            "filters": [],
            "layers": [],
            "interactionConfig": {
                "tooltip": {"fieldsToShow": {}, "enabled": True},
                "brush": {"size": 0.5, "enabled": False},
                "geocoder": {"enabled": False},
                "coordinate": {"enabled": True}
            }
        },
        "uiState": {
            "readOnly": False,
            "activeSidePanel": "layer",
            "currentModal": None,
            "mapControls": {
                "visibleLayers": {"show": True},
                "mapLegend": {"show": True},
                "toggle3d": {"show": True},
                "splitMap": {"show": False}
            }
        }
    }
}