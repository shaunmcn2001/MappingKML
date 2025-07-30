import xml.etree.ElementTree as ET
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import kml_utils as kml


def test_hex_to_kml_color_valid():
    assert kml._hex_to_kml_color("#FF0000", 0.5) == "7f0000FF"
    assert kml._hex_to_kml_color("00FF00", 1.0) == "ff00FF00"


def test_hex_to_kml_color_invalid():
    assert kml._hex_to_kml_color("fff", 1.0) == "ffFFFFFF"


def build_feature(region="QLD"):
    if region == "QLD":
        return {
            "type": "Feature",
            "properties": {"lot": "1", "plan": "RP12345"},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[150.0, -28.0], [150.1, -28.0], [150.1, -28.1], [150.0, -28.1], [150.0, -28.0]]],
            },
        }
    return {
        "type": "Feature",
        "properties": {"lotnumber": "2", "sectionnumber": "1", "planlabel": "DP67890"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [[[150.2, -33.8], [150.3, -33.8], [150.3, -33.9], [150.2, -33.9], [150.2, -33.8]]],
        },
    }


def parse_kml(kml_str):
    return ET.fromstring(kml_str)


def test_generate_kml_qld():
    feat = build_feature("QLD")
    result = kml.generate_kml([feat], "QLD", "#123456", 0.3, "#654321", 2, "Test")
    root = parse_kml(result)
    ns = {"k": "http://www.opengis.net/kml/2.2"}
    name = root.find(".//k:Placemark/k:name", ns)
    assert name is not None and name.text == "1RP12345"
    fill = root.find(".//k:PolyStyle/k:color", ns)
    assert fill.text == kml._hex_to_kml_color("#123456", 0.3)


def test_generate_kml_nsw():
    feat = build_feature("NSW")
    result = kml.generate_kml([feat], "NSW", "#abcdef", 0.8, "#000000", 1, "Test")
    root = parse_kml(result)
    ns = {"k": "http://www.opengis.net/kml/2.2"}
    name = root.find(".//k:Placemark/k:name", ns)
    assert name is not None and name.text == "2DP67890"
    fill = root.find(".//k:PolyStyle/k:color", ns)
    assert fill.text == kml._hex_to_kml_color("#abcdef", 0.8)
