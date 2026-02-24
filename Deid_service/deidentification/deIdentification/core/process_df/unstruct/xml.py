import re
import xml.etree.ElementTree as ET
from dateutil import parser as date_parser
from deIdentification.nd_logger import nd_logger
from core.process_df.unstruct.xml_utils import ZIPCODE_TAGS, DOB_TAGS

def deidentify_xml_tags(text: str, tag_replacements: dict) -> str:
    """
    De-identify XML string values based on tag names and attributes,
    with special handling for DOB and ZIP.
    Masks both tag text and 'value' attributes.
    """
    if not isinstance(text, str) or not text.strip().startswith("<"):
        return text

    try:
        root = ET.fromstring(text)
    except Exception as e:
        nd_logger.debug(f"[XMLUtils] Skipping invalid XML: {e}")
        return text

    for tag in root.iter():
        tag_name = tag.tag.split("}")[-1]  # strip namespace if present
        val = tag.text.strip() if tag.text else ""

        # --- Special Rule: DOB ---
        if tag_name.lower() in [t.lower() for t in DOB_TAGS]:
            try:
                parsed = date_parser.parse(val)
                tag.text = str(parsed.year)  # only keep year
            except Exception as e:
                nd_logger.debug(f"[XMLUtils] DOB parse failed: {val} ({e})")

        # --- Special Rule: ZIP / ADDRESS ---
        if tag_name.lower() in [t.lower() for t in ZIPCODE_TAGS]:
            def mask_zip(match):
                return match.group(0)[:3]  # keep first 3 digits only
            masked_val = re.sub(r"\d{5}", mask_zip, val)
            tag.text = masked_val

        # --- General Replacements ---
        if tag_name in tag_replacements:
            masked_value = tag_replacements[tag_name]
            tag.text = masked_value

            # Also mask 'value' attribute if present
            if "value" in tag.attrib:
                tag.attrib["value"] = masked_value

        # --- Optional: Mask attributes matching sensitive patterns ---
        for attr_name, attr_val in list(tag.attrib.items()):
            if attr_name.lower() in [t.lower() for t in tag_replacements]:
                tag.attrib[attr_name] = tag_replacements[attr_name]

    # ✅ Preserve namespaces
    ET.register_namespace("SOAP-ENV", "http://schemas.xmlsoap.org/soap/envelope/")
    ET.register_namespace("xsd", "http://www.w3.org/2001/XMLSchema")
    ET.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

    # ✅ Return pretty XML string
    return ET.tostring(root, encoding="utf-8", xml_declaration=True).decode("utf-8")
